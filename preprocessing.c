/*
 *******************************************************************************
 *
 *	QUERY PREPROCESSING HOOKS
 *
 * The main point of this module is to recognize somehow settings for the query.
 * It may be considered as user interface.
 *
 * The configurable settings are:
 *		'query_hash': hash of the type of the given query
 *		'use_aqo': whether to use AQO estimations in query optimization
 *		'learn_aqo': whether to update AQO data based on query execution
 *					 statistics
 *		'fs': hash of feature space to use with given query
 *		'auto_tuning': whether AQO may change use_aqo and learn_aqo values
 *					   for the next execution of such type of query using
 *					   its self-tuning algorithm
 *
 * Currently the module works as follows:
 * 1. Query type determination. We consider that two queries are of the same
 *		type if and only if they are equal or their difference is only in their
 *		constants. We use hash function, which returns the same value for all
 *		queries of the same type. This typing strategy is not the only possible
 *		one for adaptive query optimization. One can easily implement another
 *		typing strategy by changing hash function.
 * 2. New query type proceeding. The handling policy for new query types is
 *		contained in variable 'aqo.mode'. It accepts five values:
 *		"intelligent", "forced", "controlled", "learn" and "disabled".
 *		Intelligent linking strategy means that for each new query type the new
 *		separate feature space is created. The hash of new feature space is
 *		set the same as the hash of new query type. Auto tuning is on by
 *		default in this mode. AQO also automatically memorizes query hash to
 *		query text mapping in aqo_query_texts table.
 *		Forced linking strategy means that every new query type is linked to
 *		the common feature space with hash 0, but we don't memorize
 *		the hash and the settings for this query.
 *		Controlled linking strategy means that new query types do not induce
 *		new feature spaces neither interact AQO somehow. In this mode the
 *		configuration of settings for different query types lies completely on
 *		user.
 *		Learn linking strategy is the same as intelligent one. The only
 *		difference is the default settings for the new query type:
 *		auto tuning is disabled.
 *		Disabled strategy means that AQO is disabled for all queries.
 * 3. For given query type we determine its query_hash, use_aqo, learn_aqo,
 *		fspace_hash and auto_tuning parameters.
 * 4. For given fspace_hash we may use its machine learning settings, but now
 *		the machine learning setting are fixed for all feature spaces.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2022, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/preprocessing.c
 *
 */

#include "postgres.h"

#include "access/parallel.h"
#include "access/table.h"
#include "commands/extension.h"
#include "parser/scansup.h"
#include "aqo.h"
#include "hash.h"
#include "preprocessing.h"
#include "storage.h"


/* List of feature spaces, that are processing in this backend. */
List *cur_classes = NIL;

int aqo_join_threshold = 0;

static bool isQueryUsingSystemRelation(Query *query);
static bool isQueryUsingSystemRelation_walker(Node *node, void *context);

/*
 * Calls standard query planner or its previous hook.
 */
static PlannedStmt *
call_default_planner(Query *parse,
					 const char *query_string,
					 int cursorOptions,
					 ParamListInfo boundParams)
{
	if (prev_planner_hook)
		return prev_planner_hook(parse,
								 query_string,
								 cursorOptions,
								 boundParams);
	else
		return standard_planner(parse,
								query_string,
								cursorOptions,
								boundParams);
}

/*
 * Can AQO be used for the query?
 */
static bool
aqoIsEnabled(Query *parse)
{
	if (creating_extension ||
		(aqo_mode == AQO_MODE_DISABLED && !force_collect_stat) ||
		(parse->commandType != CMD_SELECT && parse->commandType != CMD_INSERT &&
		parse->commandType != CMD_UPDATE && parse->commandType != CMD_DELETE))
		return false;

	return true;
}

/*
 * Before query optimization we determine machine learning settings
 * for the query.
 * This hook computes query_hash, and sets values of learn_aqo,
 * use_aqo and is_common flags for given query.
 * Creates an entry in aqo_queries for new type of query if it is
 * necessary, i. e. AQO mode is "intelligent".
 */
PlannedStmt *
aqo_planner(Query *parse,
			const char *query_string,
			int cursorOptions,
			ParamListInfo boundParams)
{
	bool			query_is_stored = false;
	MemoryContext	oldCxt;

	 /*
	  * We do not work inside an parallel worker now by reason of insert into
	  * the heap during planning. Transactions are synchronized between parallel
	  * sections. See GetCurrentCommandId() comments also.
	  */
	if (!aqoIsEnabled(parse) ||
		IsInParallelMode() || IsParallelWorker() ||
		strstr(application_name, "postgres_fdw") != NULL || /* Prevent distributed deadlocks */
		strstr(application_name, "pgfdw:") != NULL || /* caused by fdw */
		isQueryUsingSystemRelation(parse) ||
		RecoveryInProgress())
	{
		/*
		 * We should disable AQO for this query to remember this decision along
		 * all execution stages.
		 */
		disable_aqo_for_query();

		return call_default_planner(parse,
									query_string,
									cursorOptions,
									boundParams);
	}

	selectivity_cache_clear();
	query_context.query_hash = get_query_hash(parse, query_string);

	/* By default, they should be equal */
	query_context.fspace_hash = query_context.query_hash;

	if (query_is_deactivated(query_context.query_hash) ||
		list_member_uint64(cur_classes,query_context.query_hash))
	{
		/*
		 * Disable AQO for deactivated query or for query belonged to a
		 * feature space, that is processing yet (disallow invalidation
		 * recursion, as an example).
		 */
		disable_aqo_for_query();

		return call_default_planner(parse,
									query_string,
									cursorOptions,
									boundParams);
	}

	elog(DEBUG1, "AQO will be used for query '%s', class "UINT64_FORMAT,
		 query_string ? query_string : "null string", query_context.query_hash);

	oldCxt = MemoryContextSwitchTo(AQOMemoryContext);
	cur_classes = lappend_uint64(cur_classes, query_context.query_hash);
	MemoryContextSwitchTo(oldCxt);

	if (aqo_mode == AQO_MODE_DISABLED)
	{
		/* Skip access to a database in this mode. */
		disable_aqo_for_query();
		goto ignore_query_settings;
	}

	query_is_stored = aqo_queries_find(query_context.query_hash, &query_context);

	if (!query_is_stored)
	{
		switch (aqo_mode)
		{
			case AQO_MODE_INTELLIGENT:
				query_context.adding_query = true;
				query_context.learn_aqo = true;
				query_context.use_aqo = false;
				query_context.auto_tuning = true;
				query_context.collect_stat = true;
				break;
			case AQO_MODE_FORCED:
				query_context.adding_query = false;
				query_context.learn_aqo = true;
				query_context.use_aqo = true;
				query_context.auto_tuning = false;
				query_context.fspace_hash = 0; /* Use common feature space */
				query_context.collect_stat = false;
				break;
			case AQO_MODE_CONTROLLED:
			case AQO_MODE_FROZEN:
				/*
				 * if query is not in the AQO knowledge base than disable AQO
				 * for this query.
				 */
				query_context.adding_query = false;
				query_context.learn_aqo = false;
				query_context.use_aqo = false;
				query_context.auto_tuning = false;
				query_context.collect_stat = false;
				break;
			case AQO_MODE_LEARN:
				query_context.adding_query = true;
				query_context.learn_aqo = true;
				query_context.use_aqo = true;
				query_context.auto_tuning = false;
				query_context.collect_stat = true;
				break;
			case AQO_MODE_DISABLED:
				/* Should never happen */
				Assert(0);
				break;
			default:
				elog(ERROR, "unrecognized mode in AQO: %d", aqo_mode);
				break;
		}
	}
	else /* Query class exists in a ML knowledge base. */
	{
		query_context.adding_query = false;

		/* Other query_context fields filled in the find_query() routine. */

		/*
		 * Deactivate query if no one reason exists for usage of an AQO machinery.
		 */
		if (!query_context.learn_aqo && !query_context.use_aqo &&
			!query_context.auto_tuning && !force_collect_stat)
			add_deactivated_query(query_context.query_hash);

		/*
		 * That we can do if query exists in database.
		 * Additional preference changes, based on AQO mode.
		 */
		switch (aqo_mode)
		{
		case AQO_MODE_FROZEN:
			/*
			 * In this mode we will suppress all writings to the knowledge base.
			 * AQO will be used for all known queries, if it is not suppressed.
			 */
			query_context.learn_aqo = false;
			query_context.auto_tuning = false;
			query_context.collect_stat = false;
			break;

		case AQO_MODE_LEARN:
			/*
			 * In this mode we want to learn with incoming query (if it is not
			 * suppressed manually) and collect stats.
			 */
			query_context.collect_stat = true;
			break;

		case AQO_MODE_INTELLIGENT:
		case AQO_MODE_FORCED:
		case AQO_MODE_CONTROLLED:
		case AQO_MODE_DISABLED:
			/* Use preferences as set early. */
			break;

		default:
			elog(ERROR, "Unrecognized aqo mode %d", aqo_mode);
		}
	}

ignore_query_settings:
	if (!query_is_stored && (query_context.adding_query || force_collect_stat))
	{
		/*
		 * Add query into the AQO knowledge base. To process an error with
		 * concurrent addition from another backend we will try to restart
		 * preprocessing routine.
		 */
		if (aqo_queries_store(query_context.query_hash, query_context.fspace_hash,
						  query_context.learn_aqo, query_context.use_aqo,
						  query_context.auto_tuning))
		{
			/*
			 * Add query text into the ML-knowledge base. Just for further
			 * analysis. In the case of cached plans we may have NULL query text.
			 */
			if (!aqo_qtext_store(query_context.query_hash, query_string))
			{
				Assert(0); /* panic only on debug installation */
				elog(ERROR, "[AQO] Impossible situation was detected. Maybe not enough of shared memory?");
			}
		}
		else
		{
			/*
			 * In the case of problems (shmem overflow, as a typical issue) -
			 * disable AQO for the query class.
			 */
			disable_aqo_for_query();

			/*
			 * Switch AQO to controlled mode. In this mode we wouldn't add new
			 * query classes, just use and learn on existed set.
			 */
			aqo_mode = AQO_MODE_CONTROLLED;
		}
	}

	if (force_collect_stat)
		/*
		 * If this GUC is set, AQO will analyze query results and collect
		 * query execution statistics in any mode.
		 */
		query_context.collect_stat = true;

	if (!IsQueryDisabled())
		/* It's good place to set timestamp of start of a planning process. */
		INSTR_TIME_SET_CURRENT(query_context.start_planning_time);

	return call_default_planner(parse,
								query_string,
								cursorOptions,
								boundParams);
}

/*
 * Turn off all AQO functionality for the current query.
 */
void
disable_aqo_for_query(void)
{
	query_context.learn_aqo = false;
	query_context.use_aqo = false;
	query_context.auto_tuning = false;
	query_context.collect_stat = false;
	query_context.adding_query = false;
	query_context.explain_only = false;

	INSTR_TIME_SET_ZERO(query_context.start_planning_time);
	query_context.planning_time = -1.;
}

typedef struct AQOPreWalkerCtx
{
	bool	trivQuery;
	int		njoins;
} AQOPreWalkerCtx;

/*
 * Examine a fully-parsed query, and return TRUE iff any relation underlying
 * the query is a system relation or no one permanent (non-temporary) relation
 * touched by the query.
 */
static bool
isQueryUsingSystemRelation(Query *query)
{
	AQOPreWalkerCtx ctx;
	bool result;

	ctx.trivQuery = true;
	ctx.njoins = 0;
	result = isQueryUsingSystemRelation_walker((Node *) query, &ctx);

	if (result || ctx.trivQuery || ctx.njoins < aqo_join_threshold)
		return true;
	return false;
}


static bool
IsAQORelation(Relation rel)
{
	char *relname = NameStr(rel->rd_rel->relname);

	if (strcmp(relname, "aqo_data") == 0 ||
		strcmp(relname, "aqo_query_texts") == 0 ||
		strcmp(relname, "aqo_query_stat") == 0 ||
		strcmp(relname, "aqo_queries") == 0
	   )
	   return true;

	return false;
}

/*
 * Walk through jointree and calculate number of potential joins
 */
static void
jointree_walker(Node *jtnode, void *context)
{
	AQOPreWalkerCtx   *ctx = (AQOPreWalkerCtx *) context;

	if (jtnode == NULL || IsA(jtnode, RangeTblRef))
		return;
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;

		/* Count number of potential joins by number of sources in FROM list */
		ctx->njoins += list_length(f->fromlist) - 1;

		foreach(l, f->fromlist)
			jointree_walker(lfirst(l), context);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		/* Don't forget about explicit JOIN statement */
		ctx->njoins++;
		jointree_walker(j->larg, context);
		jointree_walker(j->rarg, context);
	}
	else
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));
	return;
}

static bool
isQueryUsingSystemRelation_walker(Node *node, void *context)
{
	AQOPreWalkerCtx   *ctx = (AQOPreWalkerCtx *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query			  *query = (Query *) node;
		ListCell		  *rtable;

		foreach(rtable, query->rtable)
		{
			RangeTblEntry *rte = lfirst(rtable);

			if (rte->rtekind == RTE_RELATION)
			{
				Relation	rel = table_open(rte->relid, AccessShareLock);
				bool		is_catalog = IsCatalogRelation(rel);
				bool		is_aqo_rel = IsAQORelation(rel);

				if (is_catalog || is_aqo_rel)
				{
					table_close(rel, AccessShareLock);
					return true;
				}

				if (rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
					/* Plane non TEMP table */
					ctx->trivQuery = false;

				table_close(rel, AccessShareLock);
			}
			else if (rte->rtekind == RTE_FUNCTION)
			{
				/*
				 * TODO: Exclude queries with AQO functions.
				 */
			}
		}

		jointree_walker((Node *) query->jointree, context);

		/* Recursively plunge into subqueries and CTEs */
		return query_tree_walker(query,
								 isQueryUsingSystemRelation_walker,
								 context,
								 0);
	}

	return expression_tree_walker(node,
								  isQueryUsingSystemRelation_walker,
								  context);
}
