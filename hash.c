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
 * Copyright (c) 2016-2022, Postgres Professional
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

static void get_clauselist_args(List *clauselist, int *nargs, int **args_hash);
static int	disjoint_set_get_parent(int *p, int v);
static void disjoint_set_merge_eclasses(int *p, int v1, int v2);
static int *perform_eclasses_join(List *clauselist, int nargs, int *args_hash);

static bool is_brace(char ch);
static bool has_consts(List *lst);
static List **get_clause_args_ptr(Expr *clause);
static bool clause_is_eq_clause(Expr *clause);


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
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		clause_hashes[i] = get_clause_hash(rinfo->clause,
										   nargs, args_hash, eclass_hash);
		args = get_clause_args_ptr(rinfo->clause);
		clause_has_consts[i] = (args != NULL && has_consts(*args));
		i++;
	}

	idx = argsort(clause_hashes, n, sizeof(*clause_hashes), int_cmp);
	inverse_idx = inverse_permutation(idx, n);

	i = 0;
	foreach(lc, clauselist)
	{
		sorted_clauses[inverse_idx[i]] = clause_hashes[i];
		i++;
	}

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

	/*
	 * Generate feature subspace hash.
	 */

	clauses_hash = get_int_array_hash(sorted_clauses, n - sh);
	eclasses_hash = get_int_array_hash(eclass_hash, nargs);
	relations_hash = get_relations_hash(relsigns);
	fss_hash = get_fss_hash(clauses_hash, eclasses_hash, relations_hash);

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
get_clause_hash(Expr *clause, int nargs, int *args_hash, int *eclass_hash)
{
	Expr	   *cclause;
	List	  **args = get_clause_args_ptr(clause);
	int			arg_eclass;
	ListCell   *l;

	if (args == NULL)
		return get_node_hash((Node *) clause);

	cclause = copyObject(clause);
	args = get_clause_args_ptr(cclause);
	foreach(l, *args)
	{
		arg_eclass = get_arg_eclass(get_node_hash(lfirst(l)),
									nargs, args_hash, eclass_hash);
		if (arg_eclass != 0)
		{
			lfirst(l) = makeNode(Param);
			((Param *) lfirst(l))->paramid = arg_eclass;
		}
	}
	if (!clause_is_eq_clause(clause) || has_consts(*args))
		return get_node_hash((Node *) cclause);
	return get_node_hash((Node *) linitial(*args));
}

/*
 * Computes hash for given string.
 */
int
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
	int			hash;

	str = remove_locations(remove_consts(nodeToString(node)));
	hash = get_str_hash(str);
	pfree(str);
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
int
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
int
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
int
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

	res = replace_patterns(str, "{CONST", is_brace);
	res = replace_patterns(res, ":stmt_len", is_brace);
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
int
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
int
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
void
get_clauselist_args(List *clauselist, int *nargs, int **args_hash)
{
	RestrictInfo *rinfo;
	List	  **args;
	ListCell   *l;
	ListCell   *l2;
	int			i = 0;
	int			sh = 0;
	int			cnt = 0;

	foreach(l, clauselist)
	{
		rinfo = (RestrictInfo *) lfirst(l);
		args = get_clause_args_ptr(rinfo->clause);
		if (args != NULL && clause_is_eq_clause(rinfo->clause))
			foreach(l2, *args)
				if (!IsA(lfirst(l2), Const))
				cnt++;
	}

	*args_hash = palloc(cnt * sizeof(**args_hash));
	foreach(l, clauselist)
	{
		rinfo = (RestrictInfo *) lfirst(l);
		args = get_clause_args_ptr(rinfo->clause);
		if (args != NULL && clause_is_eq_clause(rinfo->clause))
			foreach(l2, *args)
				if (!IsA(lfirst(l2), Const))
				(*args_hash)[i++] = get_node_hash(lfirst(l2));
	}
	qsort(*args_hash, cnt, sizeof(**args_hash), int_cmp);

	for (i = 1; i < cnt; ++i)
		if ((*args_hash)[i - 1] == (*args_hash)[i])
			sh++;
		else
			(*args_hash)[i - sh] = (*args_hash)[i];

	*nargs = cnt - sh;
	*args_hash = repalloc(*args_hash, (*nargs) * sizeof(**args_hash));
}

/*
 * Returns class of an object in disjoint set.
 */
int
disjoint_set_get_parent(int *p, int v)
{
	if (p[v] == -1)
		return v;
	else
		return p[v] = disjoint_set_get_parent(p, p[v]);
}

/*
 * Merges two equivalence classes in disjoint set.
 */
void
disjoint_set_merge_eclasses(int *p, int v1, int v2)
{
	int			p1,
				p2;

	p1 = disjoint_set_get_parent(p, v1);
	p2 = disjoint_set_get_parent(p, v2);
	if (p1 != p2)
	{
		if ((v1 + v2) % 2)
			p[p1] = p2;
		else
			p[p2] = p1;
	}
}

/*
 * Constructs disjoint set on arguments.
 */
int *
perform_eclasses_join(List *clauselist, int nargs, int *args_hash)
{
	RestrictInfo *rinfo;
	int		   *p;
	ListCell   *l,
			   *l2;
	List	  **args;
	int			h2;
	int			i2,
				i3;

	p = palloc(nargs * sizeof(*p));
	memset(p, -1, nargs * sizeof(*p));

	foreach(l, clauselist)
	{
		rinfo = (RestrictInfo *) lfirst(l);
		args = get_clause_args_ptr(rinfo->clause);
		if (args != NULL && clause_is_eq_clause(rinfo->clause))
		{
			i3 = -1;
			foreach(l2, *args)
			{
				if (!IsA(lfirst(l2), Const))
				{
					h2 = get_node_hash(lfirst(l2));
					i2 = get_id_in_sorted_int_array(h2, nargs, args_hash);
					if (i3 != -1)
						disjoint_set_merge_eclasses(p, i2, i3);
					i3 = i2;
				}
			}
		}
	}

	return p;
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
	int			i,
				v;
	int		   *e_hashes;

	get_clauselist_args(clauselist, nargs, args_hash);
	*eclass_hash = palloc((*nargs) * sizeof(**eclass_hash));

	p = perform_eclasses_join(clauselist, *nargs, *args_hash);
	lsts = palloc((*nargs) * sizeof(*lsts));
	e_hashes = palloc((*nargs) * sizeof(*e_hashes));

	for (i = 0; i < *nargs; ++i)
		lsts[i] = NIL;

	for (i = 0; i < *nargs; ++i)
	{
		v = disjoint_set_get_parent(p, i);
		lsts[v] = lappend_int(lsts[v], (*args_hash)[i]);
	}
	for (i = 0; i < *nargs; ++i)
		e_hashes[i] = get_unordered_int_list_hash(lsts[i]);

	for (i = 0; i < *nargs; ++i)
		(*eclass_hash)[i] = e_hashes[disjoint_set_get_parent(p, i)];
}

/*
 * Checks whether the given char is brace, i. e. '{' or '}'.
 */
bool
is_brace(char ch)
{
	return ch == '{' || ch == '}';
}

/*
 * Returns whether arguments list contain constants.
 */
bool
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
List **
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

/*
 * Returns whether the clause is an equivalence clause.
 */
bool
clause_is_eq_clause(Expr *clause)
{
	/* TODO: fix this horrible mess */
	return (
			clause->type == T_OpExpr ||
			clause->type == T_DistinctExpr ||
			clause->type == T_NullIfExpr ||
			clause->type == T_ScalarArrayOpExpr
		) && (
			  ((OpExpr *) clause)->opno == Int4EqualOperator ||
			  ((OpExpr *) clause)->opno == BooleanEqualOperator ||
			  ((OpExpr *) clause)->opno == TextEqualOperator ||
			  ((OpExpr *) clause)->opno == TIDEqualOperator ||
			  ((OpExpr *) clause)->opno == ARRAY_EQ_OP ||
			  ((OpExpr *) clause)->opno == RECORD_EQ_OP ||
			  ((OpExpr *) clause)->opno == 15 ||
			  ((OpExpr *) clause)->opno == 92 ||
			  ((OpExpr *) clause)->opno == 93 ||
			  ((OpExpr *) clause)->opno == 94 ||
			  ((OpExpr *) clause)->opno == 352 ||
			  ((OpExpr *) clause)->opno == 353 ||
			  ((OpExpr *) clause)->opno == 385 ||
			  ((OpExpr *) clause)->opno == 386 ||
			  ((OpExpr *) clause)->opno == 410 ||
			  ((OpExpr *) clause)->opno == 416 ||
			  ((OpExpr *) clause)->opno == 503 ||
			  ((OpExpr *) clause)->opno == 532 ||
			  ((OpExpr *) clause)->opno == 533 ||
			  ((OpExpr *) clause)->opno == 560 ||
			  ((OpExpr *) clause)->opno == 566 ||
			  ((OpExpr *) clause)->opno == 607 ||
			  ((OpExpr *) clause)->opno == 649 ||
			  ((OpExpr *) clause)->opno == 620 ||
			  ((OpExpr *) clause)->opno == 670 ||
			  ((OpExpr *) clause)->opno == 792 ||
			  ((OpExpr *) clause)->opno == 811 ||
			  ((OpExpr *) clause)->opno == 900 ||
			  ((OpExpr *) clause)->opno == 1093 ||
			  ((OpExpr *) clause)->opno == 1108 ||
			  ((OpExpr *) clause)->opno == 1550 ||
			  ((OpExpr *) clause)->opno == 1120 ||
			  ((OpExpr *) clause)->opno == 1130 ||
			  ((OpExpr *) clause)->opno == 1320 ||
			  ((OpExpr *) clause)->opno == 1330 ||
			  ((OpExpr *) clause)->opno == 1500 ||
			  ((OpExpr *) clause)->opno == 1535 ||
			  ((OpExpr *) clause)->opno == 1616 ||
			  ((OpExpr *) clause)->opno == 1220 ||
			  ((OpExpr *) clause)->opno == 1201 ||
			  ((OpExpr *) clause)->opno == 1752 ||
			  ((OpExpr *) clause)->opno == 1784 ||
			  ((OpExpr *) clause)->opno == 1804 ||
			  ((OpExpr *) clause)->opno == 1862 ||
			  ((OpExpr *) clause)->opno == 1868 ||
			  ((OpExpr *) clause)->opno == 1955 ||
			  ((OpExpr *) clause)->opno == 2060 ||
			  ((OpExpr *) clause)->opno == 2542 ||
			  ((OpExpr *) clause)->opno == 2972 ||
			  ((OpExpr *) clause)->opno == 3222 ||
			  ((OpExpr *) clause)->opno == 3516 ||
			  ((OpExpr *) clause)->opno == 3629 ||
			  ((OpExpr *) clause)->opno == 3676 ||
			  ((OpExpr *) clause)->opno == 3882 ||
			  ((OpExpr *) clause)->opno == 3240 ||
			  ((OpExpr *) clause)->opno == 3240
		);
}
