"""
Tests for sensate.pipeline.preprocessing.preprocessing_pipeline (PreprocessingPipeline).
"""
import pytest
from sensate.pipeline.preprocessing.preprocessing_pipeline import PreprocessingPipeline


@pytest.fixture
def pp():
    return PreprocessingPipeline()


class TestTokenization:
    def test_returns_list_of_tokens_for_single_query(self, pp):
        result = pp("SELECT a FROM t")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_returns_list_of_lists_for_batch(self, pp):
        result = pp(["SELECT a FROM t", "SELECT b FROM s"])
        assert isinstance(result, list)
        assert all(isinstance(r, list) for r in result)


class TestKeywordPreservation:
    SQL = "SELECT a FROM t WHERE a = 1 GROUP BY a ORDER BY a"

    def test_select_keyword_preserved(self, pp):
        tokens = pp(self.SQL)
        assert 'SELECT' in tokens

    def test_from_keyword_preserved(self, pp):
        assert 'FROM' in pp(self.SQL)

    def test_where_keyword_preserved(self, pp):
        assert 'WHERE' in pp(self.SQL)

    def test_group_by_preserved(self, pp):
        tokens = pp(self.SQL)
        assert 'GROUP' in tokens and 'BY' in tokens

    def test_order_by_preserved(self, pp):
        tokens = pp(self.SQL)
        assert 'ORDER' in tokens


class TestLiteralReplacement:
    def test_numeric_literal_replaced(self, pp):
        tokens = pp("SELECT a FROM t WHERE a = 42")
        assert '<NUM>' in tokens
        assert '42' not in tokens

    def test_string_literal_replaced(self, pp):
        tokens = pp("SELECT a FROM t WHERE a = 'hello'")
        assert '<STR>' in tokens
        assert "'hello'" not in tokens

    def test_multiple_literals_replaced(self, pp):
        tokens = pp("SELECT a FROM t WHERE a = 1 AND b = 'x'")
        assert '<NUM>' in tokens
        assert '<STR>' in tokens


class TestTableAndColumnReplacement:
    def test_table_name_replaced(self, pp):
        tokens = pp("SELECT a FROM users")
        assert '<TAB>' in tokens
        assert 'users' not in tokens

    def test_column_replaced(self, pp):
        tokens = pp("SELECT name FROM users")
        assert '<COL>' in tokens

    def test_output_alias_replaced(self, pp):
        tokens = pp("SELECT SUM(a) AS total FROM t")
        assert '<COL_OUT>' in tokens

    def test_table_alias_token_generated(self, pp):
        tokens = pp("SELECT u.name FROM users u")
        alias_tokens = [t for t in tokens if t.startswith('<ALIAS_T')]
        assert len(alias_tokens) > 0

    def test_alias_numbered_sequentially(self, pp):
        tokens = pp("SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id")
        assert '<ALIAS_T1>' in tokens
        assert '<ALIAS_T2>' in tokens


class TestJoinHandling:
    SQL = ("SELECT u.name, o.amount FROM users u "
           "JOIN orders o ON o.user_id = u.id")

    def test_join_keyword_preserved(self, pp):
        assert 'JOIN' in pp(self.SQL)

    def test_on_keyword_preserved(self, pp):
        assert 'ON' in pp(self.SQL)

    def test_both_aliases_appear(self, pp):
        tokens = pp(self.SQL)
        assert '<ALIAS_T1>' in tokens
        assert '<ALIAS_T2>' in tokens


class TestComplexQuery:
    SQL = ("SELECT u.up_name as [name], x.objID "
           "FROM #upload u JOIN #x x ON x.up_id = u.up_id "
           "JOIN PhotoObjAll AS p ON p.objID = x.objID "
           "ORDER BY x.up_id")

    def test_no_raw_table_names(self, pp):
        tokens = pp(self.SQL)
        raw_tables = {'upload', 'PhotoObjAll'}
        for t in tokens:
            assert t not in raw_tables

    def test_tab_token_present(self, pp):
        assert '<TAB>' in pp(self.SQL)

    def test_col_out_for_bracket_alias(self, pp):
        assert '<COL_OUT>' in pp(self.SQL)


class TestBatchProcessing:
    def test_batch_length_matches_input(self, pp):
        queries = ["SELECT a FROM t", "SELECT b FROM s", "SELECT c FROM r"]
        result = pp(queries)
        assert len(result) == len(queries)

    def test_each_result_is_nonempty(self, pp):
        queries = ["SELECT a FROM t", "SELECT b FROM s"]
        result = pp(queries)
        assert all(len(r) > 0 for r in result)

    def test_empty_string_returns_empty_list(self, pp):
        result = pp("")
        assert result == []


class TestReset:
    def test_aliases_reset_between_calls(self, pp):
        # First query registers alias 'u' as ALIAS_T1
        t1 = pp("SELECT u.id FROM users u")
        # Second independent call must also start fresh at ALIAS_T1
        t2 = pp("SELECT u.id FROM orders u")
        assert t1 == t2  # structurally identical queries should produce identical output
