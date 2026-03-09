"""
Tests for sensate.pipeline.preprocessing.corpus_pipeline (PairGenerator).
"""
import pytest
from sensate.pipeline.preprocessing.corpus_pipeline import PairGenerator


class TestPairGeneratorBasic:
    def setup_method(self):
        self.gen = PairGenerator(window_size=2)

    # --- return type -------------------------------------------------------
    def test_returns_list_of_tuples(self):
        pairs = self.gen.generate_center_context_pair(['a', 'b', 'c'])
        assert all(isinstance(p, tuple) and len(p) == 2 for p in pairs), \
            "Each pair must be a (center_idx, context_idx) tuple"

    def test_indices_are_ints(self):
        pairs = self.gen.generate_center_context_pair(['x', 'y', 'z'])
        for ci, ctx in pairs:
            assert isinstance(ci, int) and isinstance(ctx, int)

    # --- correctness -------------------------------------------------------
    def test_no_self_pairs(self):
        pairs = self.gen.generate_center_context_pair(['a', 'b', 'c', 'd', 'e'])
        for ci, ctx in pairs:
            assert ci != ctx, "Center and context indices must differ"

    def test_window_size_respected(self):
        tokens = ['a', 'b', 'c', 'd', 'e']
        pairs = self.gen.generate_center_context_pair(tokens)
        for ci, ctx in pairs:
            assert abs(ci - ctx) <= 2, f"Distance {abs(ci - ctx)} exceeds window_size=2"

    def test_all_valid_pairs_present_small(self):
        # 3 tokens, window=2 → every non-self pair within distance ≤2 appears
        tokens = ['a', 'b', 'c']
        pairs = set(self.gen.generate_center_context_pair(tokens))
        expected = {(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)}
        assert pairs == expected

    # --- duplicate tokens --------------------------------------------------
    def test_duplicate_tokens_get_distinct_positions(self):
        # 'SELECT' appears at positions 0 and 2 — both must appear as center_idx
        tokens = ['SELECT', 'FROM', 'SELECT']
        pairs = self.gen.generate_center_context_pair(tokens)
        center_indices = {ci for ci, _ in pairs}
        assert 0 in center_indices, "First SELECT (pos 0) must appear as center"
        assert 2 in center_indices, "Second SELECT (pos 2) must appear as center"

    def test_duplicate_tokens_distinct_context_positions(self):
        tokens = ['WHERE', 'AND', 'WHERE']
        pairs = self.gen.generate_center_context_pair(tokens)
        # Center 1 ('AND') should produce contexts at both idx 0 and idx 2
        ctx_for_center1 = {ctx for ci, ctx in pairs if ci == 1}
        assert ctx_for_center1 == {0, 2}

    # --- edge cases --------------------------------------------------------
    def test_single_token_no_pairs(self):
        assert self.gen.generate_center_context_pair(['only']) == []

    def test_two_tokens_one_pair_each_direction(self):
        pairs = set(self.gen.generate_center_context_pair(['x', 'y']))
        assert pairs == {(0, 1), (1, 0)}

    def test_empty_sentence_no_pairs(self):
        assert self.gen.generate_center_context_pair([]) == []

    # --- window_size=1 variant --------------------------------------------
    def test_window_size_one(self):
        gen1 = PairGenerator(window_size=1)
        tokens = ['a', 'b', 'c', 'd']
        pairs = gen1.generate_center_context_pair(tokens)
        for ci, ctx in pairs:
            assert abs(ci - ctx) == 1, "With window_size=1 only adjacent pairs allowed"

    # --- corpus-level call ------------------------------------------------
    def test_call_processes_all_sentences(self):
        corpus = [['a', 'b'], ['c', 'd', 'e']]
        result = self.gen(corpus)
        assert len(result) == 2
        assert len(result[0]) > 0
        assert len(result[1]) > 0

    def test_call_returns_tuples_for_each_sentence(self):
        corpus = [['SELECT', 'FROM', 'SELECT'], ['WHERE', 'AND']]
        result = self.gen(corpus)
        for sent_pairs in result:
            for p in sent_pairs:
                assert isinstance(p, tuple) and len(p) == 2
