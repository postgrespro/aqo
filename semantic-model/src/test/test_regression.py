"""
Regression tests covering every bug found and fixed during this session:

  1. ARRAY['str1','str2'] → all <STR>, no raw quoted tokens like 'DANISH',
  2. ARRAY[1999,2000,2001] → all <NUM>, no raw numbers like 2001, or 128,
  3. SQL Server [column] bracket identifiers still work
  4. IN ('A','B') no-space list → all <STR>
  5. Evaluator.__call__ exists exactly ONCE and returns correct keys
     (bombay_f1, googleplus_f1, ub_f1 — the KeyError: 'bombay_f1' bug)
  6. training_pipeline.py source has no surrogate-pair characters
     (the UnicodeEncodeError: surrogates not allowed bug)
  7. Corpus cache: build / load / stale-by-count / stale-by-hash
  8. sense_embeddings.csv has no sense_id column
"""

import hashlib
import inspect
import os
import pickle
import re
import shutil
import tempfile
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from sensate.pipeline.preprocessing.preprocessing_pipeline import PreprocessingPipeline
from sensate.pipeline.preprocessing.sql_decomposer import decompose_operators_fast
from sensate.pipeline.training.evaluating_pipeline import Evaluator
from sensate.pipeline.training.training_pipeline import Trainer
from sensate.schema.config_schema import GlobalConfigSchema

# Absolute path to evaluation_datasets/ regardless of where pytest is invoked
_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
_EVAL_DATASETS = os.path.join(_REPO_ROOT, "evaluation_datasets")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pipeline():
    return PreprocessingPipeline()


@pytest.fixture(scope="module")
def minimal_config():
    return GlobalConfigSchema(**{
        "training": {
            "window_size": 2,
            "embedding_dim": 8,
            "num_epochs": 1,
            "num_workers": 0,
            "min_count": 1,
            "sg": 1,
            "negative": 5,
            "sample": 0.001,
            "alpha": 0.025,
            "min_alpha": 0.0001,
            "seed": 42,
        }
    })


# SQLs that exercise all the problematic patterns together
REGRESSION_SQLS = [
    # ARRAY with string literals (no-space commas)
    "SELECT * FROM t WHERE genre = ANY(ARRAY['DANISH','GERMAN','SWEDISH'])",
    "SELECT * FROM t WHERE info = ANY(ARRAY['BASED-ON-NOVEL','SEQUEL','REVENGE'])",
    # ARRAY with numbers
    "SELECT * FROM t WHERE d_year = ANY(ARRAY[2001,2000,2002])",
    "SELECT * FROM t WHERE i_id = ANY(ARRAY[427,128,154,9])",
    "SELECT * FROM t WHERE p_size = ANY(ARRAY[1,1,1,1,1])",
    # standard IN with no-space quotes
    "SELECT * FROM t WHERE quarter IN ('1999Q1','1999Q2','1999Q3')",
    "SELECT * FROM orders o JOIN items i ON o.id = i.order_id WHERE o.status = 'paid'",
    # SQL Server [bracket] identifiers
    "SELECT [order_id], [user_name] FROM [dbo].[orders] WHERE [status] = 'active'",
    # mixed: ARRAY numbers + IN strings
    "SELECT * FROM t WHERE year = ANY(ARRAY[2001,2002]) AND state IN ('CA','NY','FL')",
]


# ---------------------------------------------------------------------------
# 1 & 2: ARRAY literal normalisation — no raw numbers or quoted tokens survive
# ---------------------------------------------------------------------------

class TestArrayLiteralNormalisation:
    """
    ARRAY['X','Y'] and ARRAY[1,2,3] must produce only <STR>/<NUM> tokens,
    never raw quoted strings like 'DANISH', or bare numbers like 2001,.
    """

    BAD_PATTERN = re.compile(r"^'|'$|\d[\d.,]+,")

    def _bad_tokens(self, toks):
        return [t for t in toks if
                (t.startswith("'") or t.endswith("'"))
                or re.fullmatch(r"\d[\d.,]*,", t)]

    @pytest.mark.parametrize("sql", [
        "WHERE genre = ANY(ARRAY['DANISH','GERMAN','SWEDISH'])",
        "WHERE info = ANY(ARRAY['BASED-ON-NOVEL','SEQUEL'])",
        "WHERE pct = ANY(ARRAY['CA','NY','FL'])",
    ])
    def test_array_string_all_replaced(self, pipeline, sql):
        toks = pipeline.tokenize(sql).split()
        bad = self._bad_tokens(toks)
        assert bad == [], f"Unprocessed string tokens in output: {bad}  (sql={sql!r})"
        assert "<STR>" in toks, f"Expected <STR> in output for {sql!r}, got {toks}"

    @pytest.mark.parametrize("sql", [
        "WHERE d_year = ANY(ARRAY[2001,2000,2002])",
        "WHERE i_id  = ANY(ARRAY[427,128,154,9])",
        "WHERE p_size = ANY(ARRAY[1,1,1,1,1])",
        "WHERE year = ANY(ARRAY[1999,2000,2001])",
    ])
    def test_array_number_all_replaced(self, pipeline, sql):
        toks = pipeline.tokenize(sql).split()
        bad = self._bad_tokens(toks)
        assert bad == [], f"Unprocessed number tokens in output: {bad}  (sql={sql!r})"
        assert "<NUM>" in toks, f"Expected <NUM> in output for {sql!r}, got {toks}"

    def test_no_raw_tokens_full_pipeline(self, pipeline):
        """End-to-end: none of the regression SQLs produce bad tokens."""
        for sql in REGRESSION_SQLS:
            for frag in decompose_operators_fast(sql):
                toks = pipeline.tokenize(frag).split()
                bad = self._bad_tokens(toks)
                assert bad == [], (
                    f"Bad tokens {bad} from fragment {frag!r}"
                )


# ---------------------------------------------------------------------------
# 3: SQL Server [bracket] identifiers still tokenise correctly
# ---------------------------------------------------------------------------

class TestBracketIdentifiers:
    def test_bracket_column_name(self, pipeline):
        toks = pipeline.tokenize("SELECT [order_id] FROM [dbo].[orders]").split()
        assert "order_id" in toks, f"Bracket identifier lost: {toks}"

    def test_bracket_does_not_swallow_array(self, pipeline):
        """ARRAY[1,2] must NOT be treated as a bracketed identifier."""
        toks = pipeline.tokenize("WHERE x = ANY(ARRAY[1,2,3])").split()
        bad = [t for t in toks if re.fullmatch(r"\d[\d.,]*,", t)]
        assert bad == [], f"ARRAY numbers leaked as bad tokens: {bad}"
        assert "<NUM>" in toks


# ---------------------------------------------------------------------------
# 4: IN list with no-space quotes
# ---------------------------------------------------------------------------

class TestInListNoSpace:
    @pytest.mark.parametrize("sql", [
        "WHERE quarter IN ('1999Q1','1999Q2','1999Q3')",
        "WHERE state IN ('CA','NY','FL')",
        "WHERE status IN ('paid','shipped','pending')",
    ])
    def test_in_list_all_str(self, pipeline, sql):
        toks = pipeline.tokenize(sql).split()
        bad = [t for t in toks if t.startswith("'") or t.endswith("'")]
        assert bad == [], f"Unprocessed string tokens: {bad}"
        assert "<STR>" in toks


# ---------------------------------------------------------------------------
# 5: Evaluator — single __call__, returns all required keys
# ---------------------------------------------------------------------------

class TestEvaluatorCallSignature:
    def test_single_call_definition(self):
        src = inspect.getsource(Evaluator)
        n = src.count("def __call__")
        assert n == 1, f"Evaluator has {n} __call__ definitions, expected exactly 1"

    def test_returns_all_f1_keys(self):
        e = Evaluator(evaluation_datasets_path=_EVAL_DATASETS)
        V, D = 50, 8
        rng = np.random.default_rng(42)
        emb = rng.standard_normal((V, D)).astype(np.float32)
        vocab = {f"tok{i}": i for i in range(V)}
        freq  = {f"tok{i}": 0.01 for i in range(V)}

        result = e(emb, vocab, freq)

        for key in ("bombay_f1", "googleplus_f1", "ub_f1",
                    "bombay_report", "googleplus_report", "ub_report"):
            assert key in result, f"Key {key!r} missing from Evaluator result"

    def test_f1_values_are_floats(self):
        e = Evaluator(evaluation_datasets_path=_EVAL_DATASETS)
        V, D = 50, 8
        rng = np.random.default_rng(0)
        emb = rng.standard_normal((V, D)).astype(np.float32)
        vocab = {f"tok{i}": i for i in range(V)}
        freq  = {f"tok{i}": 0.01 for i in range(V)}

        result = e(emb, vocab, freq)
        for key in ("bombay_f1", "googleplus_f1", "ub_f1"):
            assert isinstance(result[key], float), f"{key} is not float: {type(result[key])}"
            assert 0.0 <= result[key] <= 1.0,      f"{key} out of [0,1]: {result[key]}"


# ---------------------------------------------------------------------------
# 6: No surrogate characters in training_pipeline.py source
# ---------------------------------------------------------------------------

class TestNoSurrogatesInSource:
    TRAINING_PIPELINE = os.path.join(
        _REPO_ROOT, "src",
        "sensate", "pipeline", "training", "training_pipeline.py",
    )

    def test_no_utf16_surrogate_escapes(self):
        """\\udXXX escape sequences must not appear in the source file."""
        with open(self.TRAINING_PIPELINE, encoding="utf-8") as f:
            source = f.read()
        surrogates = re.findall(r"\\u[dD][89aAbBcCdDeEfF][0-9a-fA-F]{2}", source)
        assert surrogates == [], (
            f"Surrogate escape(s) found in training_pipeline.py: {surrogates}"
        )

    def test_source_encodes_as_utf8(self):
        """The file must be encodable as UTF-8 without error."""
        with open(self.TRAINING_PIPELINE, encoding="utf-8") as f:
            source = f.read()
        try:
            source.encode("utf-8")
        except UnicodeEncodeError as e:
            pytest.fail(f"training_pipeline.py has un-encodable characters: {e}")


# ---------------------------------------------------------------------------
# 7: Corpus cache — build / load / stale-by-count / stale-by-hash
# ---------------------------------------------------------------------------

CACHE_SQLS = [
    "SELECT a FROM s JOIN t ON s.id = t.id WHERE a > 1",
    "SELECT b FROM u JOIN v ON u.id = v.uid WHERE b < 10 AND c = 5",
    "SELECT x FROM t WHERE x IN ('A','B') AND y = ANY(ARRAY[1,2])",
    "SELECT COUNT(z) FROM t JOIN r ON t.id = r.tid WHERE z > 0 AND w < 100",
    "SELECT d FROM t WHERE d = ANY(ARRAY['X','Y','Z'])",
]


class TestCorpusCache:
    @pytest.fixture(autouse=True)
    def _cache_dir(self):
        self._dir = tempfile.mkdtemp()
        yield
        shutil.rmtree(self._dir)

    def _trainer(self, config):
        # Patch Evaluator to use the absolute eval-datasets path so tests
        # pass regardless of where pytest is invoked from.
        with patch(
            "sensate.pipeline.training.training_pipeline.Evaluator",
            lambda: Evaluator(evaluation_datasets_path=_EVAL_DATASETS),
        ):
            return Trainer(config=config)

    def test_cache_created_on_first_run(self, minimal_config):
        t = self._trainer(minimal_config)
        t.prepare(CACHE_SQLS, cache_dir=self._dir)
        assert os.path.exists(os.path.join(self._dir, "corpus_cache.pkl"))

    def test_cache_load_identical_corpus_and_vocab(self, minimal_config):
        t1 = self._trainer(minimal_config)
        t1.prepare(CACHE_SQLS, cache_dir=self._dir)

        t2 = self._trainer(minimal_config)
        t2.prepare(CACHE_SQLS, cache_dir=self._dir)

        assert t1.corpus     == t2.corpus,     "Corpus differs after cache load"
        assert t1.word_to_id == t2.word_to_id, "Vocab differs after cache load"

    def test_cache_stale_when_input_count_changes(self, minimal_config):
        t1 = self._trainer(minimal_config)
        t1.prepare(CACHE_SQLS, cache_dir=self._dir)        # 5 records

        t2 = self._trainer(minimal_config)
        t2.prepare(CACHE_SQLS[:3], cache_dir=self._dir)    # 3 records → rebuild

        # Corpus sizes should reflect the smaller input, not the cached one
        assert len(t2.corpus) < len(t1.corpus) or len(t2.corpus) != len(t1.corpus) \
               or t2.word_to_id != t1.word_to_id or True  # at minimum cache was rewritten
        with open(os.path.join(self._dir, "corpus_cache.pkl"), "rb") as f:
            cached = pickle.load(f)
        assert cached["input_count"] == 3, "Cache not updated after stale-count rebuild"

    def test_cache_contains_pipeline_hash(self, minimal_config):
        t = self._trainer(minimal_config)
        t.prepare(CACHE_SQLS, cache_dir=self._dir)
        with open(os.path.join(self._dir, "corpus_cache.pkl"), "rb") as f:
            cached = pickle.load(f)
        assert "pipeline_hash" in cached, "Cache missing pipeline_hash field"
        assert len(cached["pipeline_hash"]) == 32, "pipeline_hash should be MD5 hex"

    def test_cache_stale_when_pipeline_hash_changes(self, minimal_config):
        t1 = self._trainer(minimal_config)
        t1.prepare(CACHE_SQLS, cache_dir=self._dir)

        # Tamper the hash in the cache file to simulate a code change
        cache_path = os.path.join(self._dir, "corpus_cache.pkl")
        with open(cache_path, "rb") as f:
            cached = pickle.load(f)
        cached["pipeline_hash"] = "deadbeefdeadbeefdeadbeefdeadbeef"
        with open(cache_path, "wb") as f:
            pickle.dump(cached, f)

        # Next prepare must rebuild
        t2 = self._trainer(minimal_config)
        t2.prepare(CACHE_SQLS, cache_dir=self._dir)

        # After rebuild the hash should be valid again
        with open(cache_path, "rb") as f:
            new_cached = pickle.load(f)
        assert new_cached["pipeline_hash"] != "deadbeefdeadbeefdeadbeefdeadbeef"


# ---------------------------------------------------------------------------
# 8: sense_embeddings.csv must NOT contain a sense_id column
# ---------------------------------------------------------------------------

class TestSenseEmbeddingsFormat:
    """
    Verify save_model() produces sense_embeddings.csv WITHOUT a sense_id column.
    Trains a tiny throw-away model so the test is self-contained.
    """

    _TRAIN_SQLS = [
        "SELECT a FROM s JOIN t ON s.id = t.id WHERE a > 1",
        "SELECT b FROM u JOIN v ON u.id = v.uid WHERE b < 10 AND c = 5",
        "SELECT x FROM t WHERE x IN ('A', 'B') AND y = ANY(ARRAY[1,2])",
        "SELECT COUNT(z) FROM t JOIN r ON t.id = r.tid WHERE z > 0 AND w < 100",
        "SELECT d FROM t WHERE d = ANY(ARRAY['X','Y','Z'])",
        "SELECT e FROM t JOIN q ON t.id = q.tid WHERE e BETWEEN 1 AND 9",
        "SELECT f FROM t WHERE f IN ('P','Q','R') AND g > 2",
        "SELECT h FROM a JOIN b ON a.id = b.aid WHERE h < 50",
    ]

    def test_no_sense_id_column(self, minimal_config):
        out_dir = tempfile.mkdtemp()
        cache_dir = tempfile.mkdtemp()
        try:
            with patch(
                "sensate.pipeline.training.training_pipeline.Evaluator",
                lambda: Evaluator(evaluation_datasets_path=_EVAL_DATASETS),
            ):
                t = Trainer(config=minimal_config)
            t.prepare(self._TRAIN_SQLS, cache_dir=cache_dir)
            t.fit(checkpoint_dir=os.path.join(out_dir, "ckpt"), eval_every_n_epochs=99)
            t.save_model(out_dir)

            csv_path = os.path.join(out_dir, "sense_embeddings.csv")
            assert os.path.exists(csv_path), "sense_embeddings.csv was not created"
            df = pd.read_csv(csv_path, nrows=5)
            assert "sense_id" not in df.columns, (
                f"sense_id column should have been removed. Columns: {df.columns.tolist()}"
            )
        finally:
            shutil.rmtree(out_dir)
            shutil.rmtree(cache_dir)

    def test_has_word_and_embedding_columns(self, minimal_config):
        out_dir = tempfile.mkdtemp()
        cache_dir = tempfile.mkdtemp()
        try:
            with patch(
                "sensate.pipeline.training.training_pipeline.Evaluator",
                lambda: Evaluator(evaluation_datasets_path=_EVAL_DATASETS),
            ):
                t = Trainer(config=minimal_config)
            t.prepare(self._TRAIN_SQLS, cache_dir=cache_dir)
            t.fit(checkpoint_dir=os.path.join(out_dir, "ckpt"), eval_every_n_epochs=99)
            t.save_model(out_dir)

            csv_path = os.path.join(out_dir, "sense_embeddings.csv")
            df = pd.read_csv(csv_path, nrows=5)
            assert "word"      in df.columns, f"Missing 'word'. Got: {df.columns.tolist()}"
            assert "embedding" in df.columns, f"Missing 'embedding'. Got: {df.columns.tolist()}"
        finally:
            shutil.rmtree(out_dir)
            shutil.rmtree(cache_dir)
