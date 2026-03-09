"""
Tests for sensate.pipeline.training.training_pipeline (Trainer).

Covers Trainer.prepare() — vocabulary construction, corpus processing,
and internal state consistency — without requiring GPU or HuggingFace auth.
"""
import pytest
import pandas as pd

from sensate.pipeline.training.training_pipeline import Trainer
from sensate.schema.config_schema import GlobalConfigSchema


# ---------------------------------------------------------------------------
# Minimal config fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def config():
    return GlobalConfigSchema(**{
        'training': {
            'learning_rate': 0.01,
            'batch_size': 4,
            'window_size': 2,
            'embedding_dim': 8,
            'num_senses': 2,
            'num_epochs': 1,
            'num_workers': 0,
        }
    })


@pytest.fixture
def trainer(config):
    return Trainer(config=config)


# Minimal SQL statements (already tokenised by PreprocessingPipeline inside prepare())
STATEMENTS = [
    "SELECT a FROM t WHERE a = 1",
    "SELECT b FROM s JOIN t ON s.id = t.id",
    "SELECT COUNT ( a ) FROM t GROUP BY a",
    "SELECT a , b FROM t WHERE a > 1 AND b < 2",
]


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------
class TestTrainerInit:
    def test_trainer_requires_config(self):
        with pytest.raises(AssertionError):
            Trainer(config=None)

    def test_trainer_initial_state(self, trainer):
        assert trainer.corpus is None
        assert trainer.vocab_table is None
        assert trainer.word_to_id is None
        assert trainer.model is None
        assert trainer.best_avg_f1 == -1.0


# ---------------------------------------------------------------------------
# prepare() — vocabulary
# ---------------------------------------------------------------------------
class TestTrainerPrepare:
    @pytest.fixture(autouse=True)
    def _prepare(self, trainer):
        trainer.prepare(STATEMENTS)
        self.trainer = trainer

    def test_corpus_is_populated(self):
        assert self.trainer.corpus is not None
        assert len(self.trainer.corpus) == len(STATEMENTS)

    def test_each_sentence_is_list_of_strings(self):
        for sent in self.trainer.corpus:
            assert isinstance(sent, list)
            assert all(isinstance(t, str) for t in sent)

    def test_vocab_table_is_dataframe(self):
        vt = self.trainer.vocab_table
        assert isinstance(vt, pd.DataFrame)
        assert 'word' in vt.columns and 'id' in vt.columns

    def test_vocab_size_positive(self):
        assert len(self.trainer.vocab_table) > 0

    def test_word_to_id_is_dict(self):
        assert isinstance(self.trainer.word_to_id, dict)

    def test_word_to_id_and_vocab_table_consistent(self):
        vt = self.trainer.vocab_table
        w2i = self.trainer.word_to_id
        # Every word in vocab_table must appear in word_to_id with the same id
        for _, row in vt.iterrows():
            assert row['word'] in w2i, f"'{row['word']}' missing from word_to_id"
            assert w2i[row['word']] == row['id']

    def test_all_tokens_in_vocab(self):
        w2i = self.trainer.word_to_id
        for sent in self.trainer.corpus:
            for tok in sent:
                assert tok in w2i, f"Token '{tok}' from corpus not in word_to_id"

    def test_vocab_ids_unique(self):
        ids = list(self.trainer.word_to_id.values())
        assert len(ids) == len(set(ids)), "Vocab ids must be unique"

    def test_vocab_ids_contiguous_from_zero(self):
        ids = sorted(self.trainer.word_to_id.values())
        assert ids == list(range(len(ids))), "Vocab ids must be 0..N-1"

    def test_vocab_words_unique(self):
        words = self.trainer.vocab_table['word'].tolist()
        assert len(words) == len(set(words)), "Vocab words must be unique"

    def test_corpus_sentences_nonempty(self):
        for sent in self.trainer.corpus:
            assert len(sent) > 0, "Each preprocessed sentence must be non-empty"


# ---------------------------------------------------------------------------
# prepare() — reproducibility
# ---------------------------------------------------------------------------
class TestTrainerPrepareRepeatability:
    def test_same_data_same_vocab(self, config):
        t1 = Trainer(config=config)
        t2 = Trainer(config=config)
        t1.prepare(STATEMENTS)
        t2.prepare(STATEMENTS)
        assert t1.word_to_id == t2.word_to_id

    def test_different_data_different_vocab(self, config):
        t1 = Trainer(config=config)
        t2 = Trainer(config=config)
        t1.prepare(["SELECT a FROM t"])
        t2.prepare(["INSERT INTO t VALUES ( 1 )"])
        # At minimum the corpus tokens will differ
        all_tokens_1 = set(w for s in t1.corpus for w in s)
        all_tokens_2 = set(w for s in t2.corpus for w in s)
        assert all_tokens_1 != all_tokens_2


# ---------------------------------------------------------------------------
# history initialisation
# ---------------------------------------------------------------------------
class TestHistoryInit:
    def test_history_keys_present(self, trainer):
        expected = {'epoch', 'train_loss', 'bombay_f1', 'googleplus_f1', 'ub_f1', 'avg_f1'}
        assert expected == set(trainer.history.keys())

    def test_history_starts_empty(self, trainer):
        for v in trainer.history.values():
            assert v == [], f"History should start empty, got {v}"
