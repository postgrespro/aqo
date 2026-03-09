import torch
import torch.nn as nn
import torch.nn.functional as F
import pandas as pd

from sensate.model.gating_network_layer import GatingNetworkLayer



class Sensate(nn.Module):
    """
    Sensate Model for multi-sense word embeddings with gating network.
    """
    def __init__(
        self,
        vocab_size: int,
        num_senses: int,
        embedding_dim: int,
        pos_weight_sigma: float = 3.0,
        alpha_w2v: float = 1.0,
        alpha_orth: float = 0.15,
        alpha_ent: float = 0.02,
        alpha_l2: float = 0.0001,
        label_smoothing: float = 0.1,
    ):
        super(Sensate, self).__init__()
        self.num_senses = num_senses
        self.embedding_dim = embedding_dim
        self.alpha_w2v = alpha_w2v
        self.alpha_orth = alpha_orth
        self.alpha_ent = alpha_ent
        self.alpha_l2 = alpha_l2
        self.label_smoothing = label_smoothing
        self.gating_network_layer = GatingNetworkLayer(pos_weight_sigma=pos_weight_sigma, d=embedding_dim)
        
        # Xavier/Glorot uniform initialization for sense embeddings [V, K, D]
        sense_embeddings_tensor = torch.empty(vocab_size, num_senses, embedding_dim)
        nn.init.xavier_uniform_(sense_embeddings_tensor.view(vocab_size * num_senses, embedding_dim))
        sense_embeddings_tensor = sense_embeddings_tensor.view(vocab_size, num_senses, embedding_dim)

        # Xavier/Glorot uniform initialization for output embeddings [V, D]
        output_embeddings_tensor = torch.empty(vocab_size, embedding_dim)
        nn.init.xavier_uniform_(output_embeddings_tensor)
        
        # Register as trainable parameters
        self.sense_embeddings = nn.Parameter(sense_embeddings_tensor)    # [V, K, D]
        self.output_embeddings = nn.Parameter(output_embeddings_tensor)  # [V, D]

        print(f"Sense Embeddings: {self.sense_embeddings.shape}")
        print(f"Output Embeddings: {self.output_embeddings.shape}")
        print("=" * 20)

    def forward(self, 
                center_pos,            # [B] - center position in each query
                context_ids,           # [B] - context word ids (single context per sample)
                query_token_ids,       # [B, T] - token ids for each position in the query
    ) -> torch.Tensor:
        batch_size = query_token_ids.shape[0]
        
        center_word_ids = query_token_ids[torch.arange(batch_size, device=query_token_ids.device), center_pos]
        center_sense_embeddings = self.sense_embeddings[center_word_ids]
        
        all_query_sense_embeddings = self.sense_embeddings[query_token_ids]  # [B, T, K, D]
        
        # Vectorized context extraction using masking
        T = query_token_ids.shape[1]
        pos_indices = torch.arange(T, device=query_token_ids.device).unsqueeze(0).expand(batch_size, -1)  # [B, T]
        context_mask = pos_indices != center_pos.unsqueeze(1)  # [B, T]
        
        # More efficient reshaping
        context_mask_expanded = context_mask.unsqueeze(-1).unsqueeze(-1)  # [B, T, 1, 1]
        masked_embeddings = all_query_sense_embeddings * context_mask_expanded  # Broadcasting
        context_sense_embeddings = masked_embeddings[context_mask].view(batch_size, T-1, self.num_senses, self.embedding_dim)  # [B, T-1, K, D]
        
        max_pooled_embedding, gating_probs = self.gating_network_layer(
            center_pos=center_pos,
            query_token_ids=query_token_ids,
            center_sense_embeddings=center_sense_embeddings,
            context_sense_embeddings=context_sense_embeddings
        )
        
        # Word2vec SG naive softmax loss with label smoothing
        logits = max_pooled_embedding @ self.output_embeddings.T
        L_w2v = F.cross_entropy(logits, context_ids, reduction='mean', label_smoothing=self.label_smoothing)
        
        # Orthogonality loss
        normalized_embeddings = F.normalize(center_sense_embeddings, p=2, dim=-1)
        similarity_matrix = torch.bmm(normalized_embeddings, normalized_embeddings.transpose(1, 2))
        triu_mask = torch.triu(torch.ones_like(similarity_matrix[0]), diagonal=1).bool()
        L_orth = (similarity_matrix[:, triu_mask].pow(2)).mean()
        
        # Entropy loss
        L_ent = -(gating_probs * torch.log(gating_probs.clamp(min=1e-12))).sum(dim=-1).mean()
        
        # L2 regularization
        L2_reg = sum(torch.norm(p, p=2)**2 for p in self.parameters())
        
        # Combine losses
        total_loss = (self.alpha_w2v * L_w2v + 
                     self.alpha_orth * L_orth + 
                     self.alpha_ent * L_ent + 
                     self.alpha_l2 * L2_reg)
        
        # Store loss components for debugging (attach to tensor)
        if hasattr(self, 'training') and self.training:
            self.last_loss_components = {
                'L_w2v': L_w2v.item(),
                'L_orth': L_orth.item(),
                'L_ent': L_ent.item(),
                'L2_reg': L2_reg.item()
            }

        return total_loss