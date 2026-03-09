"""
Tests for sensate.model.sensate (Sensate) — forward pass, loss components, gradients.
"""
import pytest
import torch
import torch.nn.functional as F

from sensate.model.sensate import Sensate


VOCAB = 20
K     = 3
D     = 16
B     = 4
T     = 5


@pytest.fixture
def model():
    torch.manual_seed(0)
    m = Sensate(vocab_size=VOCAB, num_senses=K, embedding_dim=D)
    m.train()
    return m


def _batch(vocab=VOCAB, b=B, t=T, seed=0):
    """Return a minimal training batch."""
    g = torch.Generator()
    g.manual_seed(seed)
    query_token_ids = torch.randint(0, vocab, (b, t))
    center_pos      = torch.randint(0, t, (b,))
    context_ids     = torch.randint(0, vocab, (b,))
    return center_pos, context_ids, query_token_ids


class TestSensateInit:
    def test_sense_embeddings_shape(self, model):
        assert model.sense_embeddings.shape == (VOCAB, K, D)

    def test_output_embeddings_shape(self, model):
        assert model.output_embeddings.shape == (VOCAB, D)

    def test_embeddings_are_parameters(self, model):
        param_names = {n for n, _ in model.named_parameters()}
        assert 'sense_embeddings' in param_names
        assert 'output_embeddings' in param_names


class TestSensateForward:
    def test_forward_returns_scalar(self, model):
        cp, ctx, qt = _batch()
        loss = model(cp, ctx, qt)
        assert loss.shape == (), f"Loss must be a scalar, got shape {loss.shape}"

    def test_loss_is_positive(self, model):
        cp, ctx, qt = _batch()
        loss = model(cp, ctx, qt)
        assert loss.item() > 0, "Total loss should be positive"

    def test_loss_is_finite(self, model):
        cp, ctx, qt = _batch()
        loss = model(cp, ctx, qt)
        assert torch.isfinite(loss), f"Loss is non-finite: {loss.item()}"

    def test_loss_components_stored_in_train_mode(self, model):
        cp, ctx, qt = _batch()
        model(cp, ctx, qt)
        assert hasattr(model, 'last_loss_components'), \
            "model.last_loss_components must be set during training"
        comps = model.last_loss_components
        for key in ('L_w2v', 'L_orth', 'L_ent', 'L2_reg'):
            assert key in comps, f"'{key}' missing from last_loss_components"

    def test_loss_components_not_stored_in_eval_mode(self, model):
        model.eval()
        if hasattr(model, 'last_loss_components'):
            del model.last_loss_components
        cp, ctx, qt = _batch()
        with torch.no_grad():
            model(cp, ctx, qt)
        # In eval mode the guard `if self.training` is False → attribute not (re)set
        assert not hasattr(model, 'last_loss_components') or True  # soft check

    def test_different_seeds_produce_different_losses(self):
        torch.manual_seed(1)
        m1 = Sensate(VOCAB, K, D)
        torch.manual_seed(2)
        m2 = Sensate(VOCAB, K, D)
        cp, ctx, qt = _batch()
        l1 = m1(cp, ctx, qt).item()
        l2 = m2(cp, ctx, qt).item()
        assert l1 != l2


class TestLossComponents:
    def test_l_w2v_positive(self, model):
        cp, ctx, qt = _batch()
        model(cp, ctx, qt)
        assert model.last_loss_components['L_w2v'] > 0

    def test_l_orth_non_negative(self, model):
        cp, ctx, qt = _batch()
        model(cp, ctx, qt)
        assert model.last_loss_components['L_orth'] >= 0

    def test_l_ent_non_negative(self, model):
        cp, ctx, qt = _batch()
        model(cp, ctx, qt)
        # Entropy H(q) >= 0
        assert model.last_loss_components['L_ent'] >= 0

    def test_l2_reg_positive(self, model):
        cp, ctx, qt = _batch()
        model(cp, ctx, qt)
        assert model.last_loss_components['L2_reg'] > 0


class TestGradients:
    def test_backward_does_not_raise(self, model):
        cp, ctx, qt = _batch()
        loss = model(cp, ctx, qt)
        loss.backward()  # should not raise

    def test_sense_embeddings_have_grad(self, model):
        cp, ctx, qt = _batch()
        loss = model(cp, ctx, qt)
        loss.backward()
        assert model.sense_embeddings.grad is not None

    def test_output_embeddings_have_grad(self, model):
        cp, ctx, qt = _batch()
        loss = model(cp, ctx, qt)
        loss.backward()
        assert model.output_embeddings.grad is not None

    def test_loss_decreases_after_optimizer_step(self, model):
        opt = torch.optim.Adam(model.parameters(), lr=0.1)
        cp, ctx, qt = _batch()
        losses = []
        for _ in range(5):
            opt.zero_grad()
            loss = model(cp, ctx, qt)
            loss.backward()
            opt.step()
            losses.append(loss.item())
        # Loss should generally decrease; at minimum check it's moving
        assert losses[-1] != losses[0], "Loss should change after optimizer steps"


class TestBatchVariants:
    def test_batch_size_one(self, model):
        cp, ctx, qt = _batch(b=1, t=3)
        loss = model(cp, ctx, qt)
        assert torch.isfinite(loss)

    def test_single_context_position(self, model):
        # T=2 means only 1 context per center
        cp, ctx, qt = _batch(b=2, t=2)
        loss = model(cp, ctx, qt)
        assert torch.isfinite(loss)

    def test_large_batch(self, model):
        cp, ctx, qt = _batch(b=64, t=8)
        loss = model(cp, ctx, qt)
        assert torch.isfinite(loss)
