from typing import List, Dict
from tqdm import tqdm

class PairGenerator:
    # Generate center-context pairs for Word2Vec training
    def __init__(self, window_size: int = 2):
        self.window_size = window_size

    def _get_context_indices(self, tokens: List[str], center_idx: int) -> List[int]:
        # Purpose: Extract context indices within window size for a given center index
        left_start = max(0, center_idx - self.window_size)
        right_end = min(len(tokens), center_idx + self.window_size + 1)
        return list(range(left_start, center_idx)) + list(range(center_idx + 1, right_end))

    def generate_center_context_pair(self, tokens: List[str]) -> List[tuple]:
        # Purpose: Generate pairs (center_idx, context_idx) for a single tokenized sentence.
        # Using indices instead of word strings to correctly handle duplicate tokens.
        pairs = []
        for i in range(len(tokens)):
            for j in self._get_context_indices(tokens, i):
                pairs.append((i, j))
        return pairs

    def __call__(self, corpus: list) -> list:
        # Purpose: Process a corpus to generate pairs for all sentences
        return [self.generate_center_context_pair(sentence) for sentence in tqdm(corpus, desc="Generating training samples", unit="sentence")]