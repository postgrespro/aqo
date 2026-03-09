from torch.utils.data import Dataset
from torch.nn.utils.rnn import pad_sequence
import torch
import numpy as np
from typing import List, Dict


def collate_fn(batch):
    """
    Custom collate function to handle variable-length sequences.
    Pads sequences to the same length within a batch.
    """
    center_pos = torch.stack([item['center_pos'] for item in batch])
    context_ids = torch.stack([item['context_ids'] for item in batch])

    # Pad query_token_ids to max length in batch
    query_token_ids = pad_sequence(
        [item['query_token_ids'] for item in batch],
        batch_first=True,
        padding_value=0
    )

    return {
        'center_pos': center_pos,
        'context_ids': context_ids,
        'query_token_ids': query_token_ids,
    }


class OnTheFlyDataset(Dataset):
    """
    On-the-fly W2V Skip-Gram dataset with full softmax.

    Enumerates ALL (center_pos, context_word_id) pairs at construction time
    — no random sampling, every pair contributes deterministically to the loss.
    No DataFrame tables are built; the raw corpus and vocab dict are all that's stored.
    """

    def __init__(self, corpus: List[List[str]], word_to_id: Dict[str, int], window_size: int = 2):
        # Per-sentence query token id arrays  [int64]
        self.query_token_ids: List[np.ndarray] = [
            np.array([word_to_id[w] for w in sent if w in word_to_id], dtype=np.int64)
            for sent in corpus
        ]

        # Enumerate every (sentence_idx, center_position_in_query, context_word_id) triple.
        # We use the position inside the *filtered* query token array (only in-vocab tokens)
        # so that center_pos correctly indexes into query_token_ids[s_idx].
        index_s: List[int] = []
        index_c: List[int] = []
        index_ctx: List[int] = []

        for s_idx, sent in enumerate(corpus):
            # Build filtered token list and position mapping once per sentence
            filtered = [(pos, word_to_id[w]) for pos, w in enumerate(sent) if w in word_to_id]
            n = len(filtered)
            for c_idx in range(n):
                c_pos_in_q, _ = filtered[c_idx]
                start = max(0, c_idx - window_size)
                end = min(n, c_idx + window_size + 1)
                for ctx_idx in range(start, end):
                    if ctx_idx == c_idx:
                        continue
                    _, ctx_word_id = filtered[ctx_idx]
                    index_s.append(s_idx)
                    index_c.append(c_idx)       # position inside filtered query token array
                    index_ctx.append(ctx_word_id)

        # Store as numpy arrays for fast indexing
        self._s   = np.array(index_s,   dtype=np.int32)
        self._c   = np.array(index_c,   dtype=np.int32)
        self._ctx = np.array(index_ctx, dtype=np.int32)

        print(f"   ✓ On-the-fly index built: {len(self._s)} (center, context) pairs "
              f"from {len(corpus)} sentences")

    def __len__(self) -> int:
        return len(self._s)

    def __getitem__(self, idx: int):
        s_idx      = int(self._s[idx])
        center_pos = int(self._c[idx])
        context_id = int(self._ctx[idx])

        return {
            'center_pos':      torch.tensor(center_pos, dtype=torch.long),
            'context_ids':     torch.tensor(context_id, dtype=torch.long),
            'query_token_ids': torch.from_numpy(self.query_token_ids[s_idx]).clone(),
        }