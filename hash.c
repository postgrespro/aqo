/*
 *******************************************************************************
 *
 *	HASH FUNCTIONS
 *
 * The main purpose of hash functions in this approach is two reflect objects
 * similarity. We want similar objects to be mapped into the same hash value.
 *
 * In our approach we consider that objects are similar if their difference lies
 * only in the values of their constants. We want query_hash, clause_hash and
 * fss_hash to satisfy this property.
 *
 *******************************************************************************
 *
 * Copyright (c) 2016-2023, Postgres Professional
 *
 * IDENTIFICATION
 *	  aqo/hash.c
 *
 */
#include "postgres.h"

#include "access/htup.h"
#include "common/fe_memutils.h"

#include "math.h"

#include "aqo.h"
#include "hash.h"
#include "path_utils.h"

static int	get_str_hash(const char *str);
static int	get_node_hash(Node *node);
static int	get_unsorted_unsafe_int_array_hash(int *arr, int len);
static int	get_unordered_int_list_hash(List *lst);

static int get_relations_hash(List *relsigns);
static int get_fss_hash(int clauses_hash, int eclasses_hash,
			 int relidslist_hash);

static char *replace_patterns(const char *str, const char *start_pattern,
				 bool (*end_pattern) (char ch));
static char *remove_consts(const char *str);
static char *remove_locations(const char *str);

static int	get_id_in_sorted_int_array(int val, int n, int *arr);
static int get_arg_eclass(int arg_hash, int nargs,
			   int *args_hash, int *eclass_hash);

static int *get_clauselist_args(List *clauselist, int *nargs, int **args_hash);

static bool is_brace(char ch);
static bool has_consts(List *lst);
static List **get_clause_args_ptr(Expr *clause);


/*********************************************************************************
 *
 * Because List natively works with OID, integer and a postgres node types,
 * implement separate set of functions which manages list of uint64 values
 * (need for the query hash type).
 *
 ********************************************************************************/

bool
list_member_uint64(const List *list, uint64 datum)
{
	const ListCell *cell;

	foreach(cell, list)
	{
		if (*((uint64 *)lfirst(cell)) == datum)
			return true;
	}

	return false;
}

/*
 * Deep copy of uint64 list.
 * Each element here is dynamically allocated in some memory context.
 * If we copy the list in another memctx we should allocate memory for new
 * elements too.
 */
List *
list_copy_uint64(List *list)
{
	ListCell *lc;
	List	 *nlist = NIL;

	foreach(lc, list)
	{
		uint64 *val = palloc(sizeof(uint64));

		*val = *(uint64 *) lfirst(lc);
		nlist = lappend(nlist, (void *) val);
	}

	return nlist;
}

List *
lappend_uint64(List *list, uint64 datum)
{
	uint64 *val = palloc(sizeof(uint64));

	*val = datum;
	list = lappend(list, (void *) val);
	return list;
}

/*
 * Remove element from a list and free the memory which was allocated to it.
 * Looks unconventional, but we unconventionally allocate memory on append, so
 * it maybe ok.
 */
List *
ldelete_uint64(List *list, uint64 datum)
{
	ListCell *cell;

	foreach(cell, list)
	{
		if (*((uint64 *)lfirst(cell)) == datum)
		{
			list = list_delete_ptr(list, lfirst(cell));
			return list;
		}
	}
	return list;
}

/********************************************************************************/

int
get_grouped_exprs_hash(int child_fss, List *group_exprs)
{
	ListCell	*lc;
	int			*hashes = palloc(list_length(group_exprs) * sizeof(int));
	int			i = 0;
	int			final_hashes[2];

	/* Calculate hash of each grouping expression. */
	foreach(lc, group_exprs)
	{
		Node *clause = (Node *) lfirst(lc);

		hashes[i++] = get_node_hash(clause);
	}

	/* Sort to get rid of expressions permutation. */
	qsort(hashes, i, sizeof(int), int_cmp);

	final_hashes[0] = child_fss;
	final_hashes[1] = get_int_array_hash(hashes, i);

	pfree(hashes);

	return get_int_array_hash(final_hashes, 2);
}

/*
 * For given object (clauselist, selectivities, reloids) creates feature
 * subspace:
 *		sets nfeatures
 *		creates and computes fss_hash
 *		transforms selectivities to features
 *
 * Special case for nfeatures == NULL: don't calculate features.
 */
int
get_fss_for_object(List *relsigns, List *clauselist,
				   List *selectivities, int *nfeatures, double **features)
{
	int			n;
	int		   *clause_hashes;
	int		   *sorted_clauses;
	int		   *idx;
	int		   *inverse_idx;
	bool	   *clause_has_consts;
	int			nargs;
	int		   *args_hash;
	int		   *eclass_hash;
	int			clauses_hash;
	int			eclasses_hash;
	int			relations_hash;
	List	  **args;
	ListCell   *lc;
	int			i,
				j,
				k,
				m;
	int			sh = 0,
				old_sh;
	int			fss_hash;

	n = list_length(clauselist);

	/* Check parameters state invariant. */
	Assert(n == list_length(selectivities) ||
		   (nfeatures == NULL && features == NULL));

	/*
	 * It should be allocated in a caller memory context, because it will be
	 * returned.
	 */
	if (nfeatures != NULL)
		*features = palloc0(sizeof(**features) * n);

	get_eclasses(clauselist, &nargs, &args_hash, &eclass_hash);
	clause_hashes = palloc(sizeof(*clause_hashes) * n);
	clause_has_consts = palloc(sizeof(*clause_has_consts) * n);
	sorted_clauses = palloc(sizeof(*sorted_clauses) * n);

	i = 0;
	foreach(lc, clauselist)
	{
		AQOClause *clause = (AQOClause *) lfirst(lc);

		clause_hashes[i] = get_clause_hash(clause, nargs, args_hash,
										   eclass_hash);
		args = get_clause_args_ptr(clause->clause);
		clause_has_consts[i] = (args != NULL && has_consts(*args));
		i++;
	}
	pfree(args_hash);

	idx = argsort(clause_hashes, n, sizeof(*clause_hashes), int_cmp);
	inverse_idx = inverse_permutation(idx, n);

	i = 0;
	foreach(lc, clauselist)
	{
		sorted_clauses[inverse_idx[i]] = clause_hashes[i];
		i++;
	}
	pfree(clause_hashes);

	i = 0;
	foreach(lc, selectivities)
	{
		Selectivity *s = (Selectivity *) lfirst(lc);

		if (nfeatures != NULL)
		{
			(*features)[inverse_idx[i]] = log(*s);
			Assert(!isnan(log(*s)));
			if ((*features)[inverse_idx[i]] < log_selectivity_lower_bound)
				(*features)[inverse_idx[i]] = log_selectivity_lower_bound;
		}
		i++;
	}
	pfree(inverse_idx);

	for (i = 0; i < n;)
	{
		k = 0;
		for (j = i; j < n && sorted_clauses[j] == sorted_clauses[i]; ++j)
			k += (int) clause_has_consts[idx[j]];
		m = j;
		old_sh = sh;
		for (j = i; j < n && sorted_clauses[j] == sorted_clauses[i]; ++j)
			if (clause_has_consts[idx[j]] || k + 1 == m - i)
			{
				if (nfeatures != NULL)
					(*features)[j - sh] = (*features)[j];
				sorted_clauses[j - sh] = sorted_clauses[j];
			}
			else
				sh++;

		if (nfeatures != NULL)
			qsort(&((*features)[i - old_sh]), j - sh - (i - old_sh),
				  sizeof(**features), double_cmp);
		i = j;
	}
	pfree(idx);
	pfree(clause_has_consts);

	/*
	 * Generate feature subspace hash.
	 */

	clauses_hash = get_int_array_hash(sorted_clauses, n - sh);
	eclasses_hash = get_int_array_hash(eclass_hash, nargs);
	relations_hash = get_relations_hash(relsigns);
	fss_hash = get_fss_hash(clauses_hash, eclasses_hash, relations_hash);
	pfree(sorted_clauses);
	pfree(eclass_hash);

	if (nfeatures != NULL)
	{
		*nfeatures = n - sh;
		(*features) = repalloc(*features, (*nfeatures) * sizeof(**features));
	}
	return fss_hash;
}

/*
 * Computes hash for given clause.
 * Hash is supposed to be constant-insensitive.
 * Also args-order-insensitiveness for equal clause is required.
 */
int
get_clause_hash(AQOClause *clause, int nargs, int *args_hash, int *eclass_hash)
{
	Expr	   *cclause;
	List	  **args = get_clause_args_ptr(clause->clause);
	int			arg_eclass;
	ListCell   *l;

	if (args == NULL)
		return get_node_hash((Node *) clause->clause);

	cclause = copyObject(clause->clause);
	args = get_clause_args_ptr(cclause);

	foreach(l, *args)
	{
		arg_eclass = get_arg_eclass(get_node_hash(lfirst(l)),
									nargs, args_hash, eclass_hash);
		if (arg_eclass != 0)
		{
			lfirst(l) = create_aqo_const_node(AQO_NODE_EXPR, arg_eclass);
		}
	}
	if (!clause->is_eq_clause || has_consts(*args))
		return get_node_hash((Node *) cclause);
	return get_node_hash((Node *) linitial(*args));
}

/*
 * Computes hash for given string.
 */
static int
get_str_hash(const char *str)
{
	return DatumGetInt32(hash_any((const unsigned char *) str,
								  strlen(str) * sizeof(*str)));
}

/*
 * Computes hash for given node.
 */
static int
get_node_hash(Node *node)
{
	char	   *str;
	char	   *no_consts;
	char	   *no_locations;
	int			hash;

	str = nodeToString(node);
	no_consts = remove_consts(str);
	pfree(str);
	no_locations = remove_locations(no_consts);
	pfree(no_consts);
	hash = get_str_hash(no_locations);
	pfree(no_locations);
	return hash;
}

/*
 * Computes hash for given array of ints.
 */
int
get_int_array_hash(int *arr, int len)
{
	return DatumGetInt32(hash_any((const unsigned char *) arr,
								  len * sizeof(*arr)));
}

/*
 * Computes hash for given unsorted array of ints.
 * Sorts given array in-place to compute hash.
 * The hash is order-insensitive.
 */
static int
get_unsorted_unsafe_int_array_hash(int *arr, int len)
{
	qsort(arr, len, sizeof(*arr), int_cmp);
	return get_int_array_hash(arr, len);
}

/*
 * Returns for an integer list a hash which does not depend on the order
 * of elements.
 *
 * Copies given list into array, sorts it and then computes its hash
 * using 'hash_any'.
 * Frees allocated memory before returning hash.
 */
static int
get_unordered_int_list_hash(List *lst)
{
	int			i = 0;
	int			len;
	int		   *arr;
	ListCell   *l;
	int			hash;

	len = list_length(lst);
	arr = palloc(sizeof(*arr) * len);
	foreach(l, lst)
		arr[i++] = lfirst_int(l);
	hash = get_unsorted_unsafe_int_array_hash(arr, len);
	pfree(arr);
	return hash;
}

/*
 * Returns the C-string in which the substrings of kind
 * "<start_pattern>[^<end_pattern>]*" are replaced with substring
 * "<start_pattern>".
 */
static char *
replace_patterns(const char *str, const char *start_pattern,
				 bool (*end_pattern) (char ch))
{
	int			i = 0;
	int			j = 0;
	int			start_pattern_len = strlen(start_pattern);
	char	   *res = palloc0(sizeof(*res) * (strlen(str) + 1));

	for (i = 0; str[i];)
	{
		if (i >= start_pattern_len && strncmp(&str[i - start_pattern_len],
											  start_pattern,
											  start_pattern_len) == 0)
		{
			while (str[i] && !end_pattern(str[i]))
				i++;
		}
		if (str[i])
			res[j++] = str[i++];
	}

	return res;
}

/*
 * Computes hash for given feature subspace.
 * Hash is supposed to be clause-order-insensitive.
 */
static int
get_fss_hash(int clauses_hash, int eclasses_hash, int relidslist_hash)
{
	int			hashes[3];

	hashes[0] = clauses_hash;
	hashes[1] = eclasses_hash;
	hashes[2] = relidslist_hash;
	return DatumGetInt32(hash_any((const unsigned char *) hashes,
								  3 * sizeof(*hashes)));
}

/*
 * Computes hash for given list of relations.
 * Hash is supposed to be relations-order-insensitive.
 * Each element of a list must have a String type,
 */
static int
get_relations_hash(List *relsigns)
{
	int			nhashes = 0;
	uint32	   *hashes = palloc(list_length(relsigns) * sizeof(uint32));
	ListCell   *lc;
	int			result;

	foreach(lc, relsigns)
	{
		hashes[nhashes++] = (uint32) lfirst_int(lc);
	}

	/* Sort the array to make query insensitive to input order of relations. */
	qsort(hashes, nhashes, sizeof(uint32), int_cmp);

	/* Make a final hash value */

	result = DatumGetInt32(hash_any((const unsigned char *) hashes,
									nhashes * sizeof(uint32)));
	pfree(hashes);

	return result;
}

/*
 * Returns the C-string in which the substrings of kind "{CONST.*}" are
 * replaced with substring "{CONST}".
 */
static char *
remove_consts(const char *str)
{
	char *res;
	char *tmp;

	tmp = replace_patterns(str, "{CONST", is_brace);
	res = replace_patterns(tmp, ":stmt_len", is_brace);
	pfree(tmp);
	return res;
}

/*
 * Returns the C-string in which the substrings of kind " :location.*}" are
 * replaced with substring " :location}".
 */
static char *
remove_locations(const char *str)
{
	return replace_patterns(str, " :location", is_brace);
}

/*
 * Returns index of given value in given sorted integer array
 * or -1 if not found.
 */
static int
get_id_in_sorted_int_array(int val, int n, int *arr)
{
	int		   *i;
	int			di;

	i = bsearch(&val, arr, n, sizeof(*arr), int_cmp);
	if (i == NULL)
		return -1;

	di = (unsigned char *) i - (unsigned char *) arr;
	di /= sizeof(*i);
	return di;
}

/*
 * Returns class of equivalence for given argument hash or 0 if such hash
 * does not belong to any equivalence class.
 */
static int
get_arg_eclass(int arg_hash, int nargs, int *args_hash, int *eclass_hash)
{
	int			di = get_id_in_sorted_int_array(arg_hash, nargs, args_hash);

	if (di == -1)
		return 0;
	else
		return eclass_hash[di];
}

/*
 * Builds list of non-constant arguments of equivalence clauses
 * of given clauselist.
 */
static int *
get_clauselist_args(List *clauselist, int *nargs, int **args_hash)
{
	AQOClause  *clause;
	List	  **args;
	ListCell   *l;
	int			i = 0;
	int			sh = 0;
	int			cnt = 0;
	int		   *p;
	int		   *p_sorted;
	int		   *args_hash_sorted;
	int		   *idx;

	/* Not more than 2 args in each clause from clauselist */
	*args_hash = palloc(2 * list_length(clauselist) * sizeof(**args_hash));
	p = palloc(2 * list_length(clauselist) * sizeof(*p));

	foreach(l, clauselist)
	{
		Expr   *e;

		clause = (AQOClause *) lfirst(l);
		args = get_clause_args_ptr(clause->clause);
		if (args == NULL || !clause->is_eq_clause)
			continue;

		/* Left argument */
		e = (args != NULL && list_length(*args) ? linitial(*args) : NULL);
		if (e && !IsA(e, Const))
		{
			(*args_hash)[cnt] = get_node_hash((Node *) e);
			p[cnt++] = clause->left_ec;
		}

		/* Right argument */
		e = (args != NULL && list_length(*args) >= 2 ? lsecond(*args) : NULL);
		if (e && !IsA(e, Const))
		{
			(*args_hash)[cnt] = get_node_hash((Node *) e);
			p[cnt++] = clause->right_ec;
		}
	}

	/* Use argsort for simultaniously sorting of args_hash and p arrays */
	idx = argsort(*args_hash, cnt, sizeof(**args_hash), int_cmp);

	args_hash_sorted = palloc(cnt * sizeof(*args_hash_sorted));
	p_sorted = palloc(cnt * sizeof(*p_sorted));

	for (i = 0; i < cnt; ++i)
	{
		args_hash_sorted[i] = (*args_hash)[idx[i]];
		p_sorted[i] = p[idx[i]];
	}
	pfree(idx);
	pfree(p);
	pfree(*args_hash);

	*args_hash = args_hash_sorted;

	/* Remove duplicates of the hashes */
	for (i = 1; i < cnt; ++i)
		if ((*args_hash)[i - 1] == (*args_hash)[i])
			sh++;
		else
		{
			(*args_hash)[i - sh] = (*args_hash)[i];
			p_sorted[i - sh] = p_sorted[i];
		}

	*nargs = cnt - sh;
	*args_hash = repalloc(*args_hash, (*nargs) * sizeof(**args_hash));
	p_sorted = repalloc(p_sorted, (*nargs) * sizeof(*p_sorted));

	/*
	 * Compress the values of eclasses.
	 * It is only sorted in order of args_hash.
	 * Get the indexes in ascending order of the elements.
	 */
	idx = argsort(p_sorted, *nargs, sizeof(*p_sorted), int_cmp);

	/*
	 * Remove the holes from given array.
	 * Later we can use it as indexes of args_hash.
	 */
	if (*nargs > 0)
	{
		int prev = p_sorted[idx[0]];
		p_sorted[idx[0]] = 0;
		for (i = 1; i < *nargs; i++)
		{
			int cur = p_sorted[idx[i]];
			if (cur == prev)
				p_sorted[idx[i]] = p_sorted[idx[i-1]];
			else
				p_sorted[idx[i]] = p_sorted[idx[i-1]] + 1;
			prev = cur;
		}
	}

	return p_sorted;
}

/*
 * Constructs arg_hashes and arg_hash->eclass_hash mapping for all non-constant
 * arguments of equivalence clauses of given clauselist.
 */
void
get_eclasses(List *clauselist, int *nargs, int **args_hash, int **eclass_hash)
{
	int		   *p;
	List	  **lsts;
	int			i;
	/*
	 * An auxiliary array of equivalence clauses hashes
	 * used to improve performance.
	 */
	int		   *e_hashes;

	p = get_clauselist_args(clauselist, nargs, args_hash);
	*eclass_hash = palloc((*nargs) * sizeof(**eclass_hash));

	lsts = palloc0((*nargs) * sizeof(*lsts));
	e_hashes = palloc((*nargs) * sizeof(*e_hashes));

	/* Combine args hashes corresponding to the same eclass into one list. */
	for (i = 0; i < *nargs; ++i)
		lsts[p[i]] = lappend_int(lsts[p[i]], (*args_hash)[i]);

	/* Precompute eclasses hashes only once per eclass. */
	for (i = 0; i < *nargs; ++i)
		if (lsts[i] != NIL)
			e_hashes[i] = get_unordered_int_list_hash(lsts[i]);

	/* Determine the hashes of each eclass. */
	for (i = 0; i < *nargs; ++i)
		(*eclass_hash)[i] = e_hashes[p[i]];

	pfree(e_hashes);
}

/*
 * Checks whether the given char is brace, i. e. '{' or '}'.
 */
static bool
is_brace(char ch)
{
	return ch == '{' || ch == '}';
}

/*
 * Returns whether arguments list contain constants.
 */
static bool
has_consts(List *lst)
{
	ListCell   *l;

	foreach(l, lst)
		if (IsA(lfirst(l), Const))
		return true;
	return false;
}

/*
 * Returns pointer on the args list in clause or NULL.
 */
static List **
get_clause_args_ptr(Expr *clause)
{
	switch (clause->type)
	{
		case T_OpExpr:
			return &(((OpExpr *) clause)->args);
			break;
		case T_DistinctExpr:
			return &(((DistinctExpr *) clause)->args);
			break;
		case T_NullIfExpr:
			return &(((NullIfExpr *) clause)->args);
			break;
		case T_ScalarArrayOpExpr:
			return &(((ScalarArrayOpExpr *) clause)->args);
			break;
		default:
			return NULL;
			break;
	}
}
