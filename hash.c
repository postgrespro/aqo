#include "aqo.h"

/*****************************************************************************
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
 *****************************************************************************/

static int	get_str_hash(const char *str);
static int	get_node_hash(Node *node);
static int	get_int_array_hash(int *arr, int len);
static int	get_unsorted_unsafe_int_array_hash(int *arr, int len);
static int	get_unordered_int_list_hash(List *lst);

static int	get_relidslist_hash(List *relidslist);
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

/*
 * Computes hash for given query.
 * Hash is supposed to be constant-insensitive.
 */
int
get_query_hash(Query *parse, const char *query_text)
{
	char	   *str_repr;
	int			hash;

	str_repr = remove_locations(remove_consts(nodeToString(parse)));
	hash = DatumGetInt32(hash_any((const unsigned char *) str_repr,
								  strlen(str_repr) * sizeof(*str_repr)));
	pfree(str_repr);

	return hash;
}

/*
 * For given object (clauselist, selectivities, relidslist) creates feature
 * subspace:
 *		sets nfeatures
 *		creates and computes fss_hash
 *		transforms selectivities to features
 */
void
get_fss_for_object(List *clauselist, List *selectivities, List *relidslist,
				   int *nfeatures, int *fss_hash, double **features)
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
	int			relidslist_hash;
	List	  **args;
	ListCell   *l;
	int			i,
				j,
				k,
				m;
	int			sh = 0,
				old_sh;

	n = list_length(clauselist);

	get_eclasses(clauselist, &nargs, &args_hash, &eclass_hash);

	clause_hashes = palloc(sizeof(*clause_hashes) * n);
	clause_has_consts = palloc(sizeof(*clause_has_consts) * n);
	sorted_clauses = palloc(sizeof(*sorted_clauses) * n);
	*features = palloc0(sizeof(**features) * n);

	i = 0;
	foreach(l, clauselist)
	{
		clause_hashes[i] = get_clause_hash(
										((RestrictInfo *) lfirst(l))->clause,
										   nargs, args_hash, eclass_hash);
		args = get_clause_args_ptr(((RestrictInfo *) lfirst(l))->clause);
		clause_has_consts[i] = (args != NULL && has_consts(*args));
		i++;
	}

	idx = argsort(clause_hashes, n, sizeof(*clause_hashes), int_cmp);
	inverse_idx = inverse_permutation(idx, n);

	i = 0;
	foreach(l, selectivities)
	{
		(*features)[inverse_idx[i]] = log(*((double *) (lfirst(l))));
		if ((*features)[inverse_idx[i]] < log_selectivity_lower_bound)
			(*features)[inverse_idx[i]] = log_selectivity_lower_bound;
		sorted_clauses[inverse_idx[i]] = clause_hashes[i];
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
				(*features)[j - sh] = (*features)[j];
				sorted_clauses[j - sh] = sorted_clauses[j];
			}
			else
				sh++;
		qsort(&((*features)[i - old_sh]), j - sh - (i - old_sh),
			  sizeof(**features), double_cmp);
		i = j;
	}

	*nfeatures = n - sh;
	(*features) = repalloc(*features, (*nfeatures) * sizeof(**features));

	clauses_hash = get_int_array_hash(sorted_clauses, *nfeatures);
	eclasses_hash = get_int_array_hash(eclass_hash, nargs);
	relidslist_hash = get_relidslist_hash(relidslist);

	*fss_hash = get_fss_hash(clauses_hash, eclasses_hash, relidslist_hash);

	pfree(clause_hashes);
	pfree(sorted_clauses);
	pfree(idx);
	pfree(inverse_idx);
	pfree(clause_has_consts);
	pfree(args_hash);
	pfree(eclass_hash);
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
int
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
char *
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
 * Computes hash for given list of relids.
 * Hash is supposed to be relids-order-insensitive.
 */
int
get_relidslist_hash(List *relidslist)
{
	return get_unordered_int_list_hash(relidslist);
}

/*
 * Returns the C-string in which the substrings of kind "{CONST.*}" are
 * replaced with substring "{CONST}".
 */
char *
remove_consts(const char *str)
{
	return replace_patterns(str, "{CONST", is_brace);
}

/*
 * Returns the C-string in which the substrings of kind " :location.*}" are
 * replaced with substring " :location}".
 */
char *
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

	*eclass_hash = palloc((*nargs) * sizeof(**eclass_hash));
	for (i = 0; i < *nargs; ++i)
		(*eclass_hash)[i] = e_hashes[disjoint_set_get_parent(p, i)];

	for (i = 0; i < *nargs; ++i)
		list_free(lsts[i]);
	pfree(lsts);
	pfree(p);
	pfree(e_hashes);
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
