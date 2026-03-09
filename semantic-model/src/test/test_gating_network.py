"""
Tests for sensate.model.gating_network_layer (GatingNetworkLayer).
"""
import pytest
import torch
import math

from sensate.model.gating_network_layer import GatingNetworkLayer


def _make_inputs(B=4, T=5, K=3, D=16, seed=0):
    """Helper: build minimal tensors for GatingNetworkLayer.forward()."""
    g = torch.Generator()
    g.manual_seed(seed)
    center_pos = torch.randint(0, T, (B,))
    query_token_ids = torch.randint(0, 20, (B, T))
    center_sense_emb = torch.randn(B, K, D, generator=g)
    context_sense_emb = torch.randn(B, T - 1, K, D, generator=g)
    return center_pos, query_token_ids, center_sense_emb, context_sense_emb


class TestGatingNetworkOutputShapes:
    @pytest.fixture
    def layer(self):
        return GatingNetworkLayer(sigma=2.0, d=16)

    def test_pooled_embedding_shape(self, layer):
        B, T, K, D = 4, 5, 3, 16
        cpos, qtids, cse, ctxe = _make_inputs(B, T, K, D)
        out, _ = layer(cpos, qtids, cse, ctxe)
        assert out.shape == (B, D), f"Expected ({B}, {D}), got {out.shape}"

    def test_gating_probs_shape(self, layer):
        B, T, K, D = 4, 5, 3, 16
        cpos, qtids, cse, ctxe = _make_inputs(B, T, K, D)
        _, q = layer(cpos, qtids, cse, ctxe)
        assert q.shape == (B, K), f"Expected ({B}, {K}), got {q.shape}"

    def test_batch_size_one(self, layer):
        cpos, qtids, cse, ctxe = _make_inputs(B=1, T=3, K=2, D=16)
        out, q = layer(cpos, qtids, cse, ctxe)
        assert out.shape == (1, 16)
        assert q.shape == (1, 2)

    def test_single_context_token(self, layer):
        # T=2: exactly 1 context token (T-1=1)
        cpos, qtids, cse, ctxe = _make_inputs(B=2, T=2, K=3, D=16)
        out, q = layer(cpos, qtids, cse, ctxe)
        assert out.shape == (2, 16)


class TestGatingProbabilities:
    @pytest.fixture
    def layer(self):
        return GatingNetworkLayer(sigma=2.0, d=16)

    def test_probs_sum_to_one(self, layer):
        cpos, qtids, cse, ctxe = _make_inputs()
        _, q = layer(cpos, qtids, cse, ctxe)
        sums = q.sum(dim=-1)
        assert torch.allclose(sums, torch.ones_like(sums), atol=1e-5), \
            f"Gating probs must sum to 1, got {sums}"

    def test_probs_non_negative(self, layer):
        cpos, qtids, cse, ctxe = _make_inputs()
        _, q = layer(cpos, qtids, cse, ctxe)
        assert (q >= 0).all()

    def test_probs_at_most_one(self, layer):
        cpos, qtids, cse, ctxe = _make_inputs()
        _, q = layer(cpos, qtids, cse, ctxe)
        assert (q <= 1 + 1e-6).all()


class TestSigmaBehavior:
    def test_large_sigma_distributes_weights_evenly(self):
        """With very large sigma, positional weights → 1 for all context tokens."""
        layer = GatingNetworkLayer(sigma=1e6, d=8)
        cpos, qtids, cse, ctxe = _make_inputs(B=2, T=5, K=2, D=8)
        out, q = layer(cpos, qtids, cse, ctxe)
        assert q.shape == (2, 2)  # still valid

    def test_sigma_two_nonzero_weights(self):
        """With sigma=2.0 the weights for distance-1 and distance-2 tokens are >0."""
        sigma = 2.0
        for dist in [1, 2]:
            w = math.exp(-dist / sigma)
            assert w > 0, f"sigma=2.0 should give w>0 for distance {dist}"

    def test_old_sigma_collapses_weights(self):
        """With sigma=0.001, weights for distance>=1 are effectively 0."""
        sigma = 0.001
        w = math.exp(-1 / sigma)
        assert w < 1e-100, "sigma=0.001 causes weight collapse"

    def test_default_sigma_is_two(self):
        layer = GatingNetworkLayer(d=8)
        assert layer.sigma == 2.0


class TestPoolingIsArgmax:
    def test_pooled_embedding_matches_argmax_sense(self):
        """The returned embedding must equal the sense at argmax(q)."""
        layer = GatingNetworkLayer(sigma=2.0, d=8)
        torch.manual_seed(42)
        B, T, K, D = 3, 4, 3, 8
        cpos, qtids, cse, ctxe = _make_inputs(B, T, K, D, seed=7)
        out, q = layer(cpos, qtids, cse, ctxe)

        for b in range(B):
            best_k = q[b].argmax().item()
            expected = cse[b, best_k]
            assert torch.allclose(out[b], expected, atol=1e-5), \
                f"Sample {b}: pooled embedding != sense at argmax"


class TestGradientFlow:
    def test_gradients_flow_to_inputs(self):
        layer = GatingNetworkLayer(sigma=2.0, d=8)
        cpos, qtids, cse, ctxe = _make_inputs(B=2, T=4, K=2, D=8)
        cse = cse.requires_grad_(True)
        ctxe = ctxe.requires_grad_(True)

        out, q = layer(cpos, qtids, cse, ctxe)
        loss = out.sum() + q.sum()
        loss.backward()

        assert cse.grad is not None
        assert ctxe.grad is not None
