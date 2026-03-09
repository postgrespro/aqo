import numpy as np
import pandas as pd
import os
from sklearn.metrics import classification_report

from sensate.pipeline.preprocessing.preprocessing_pipeline import PreprocessingPipeline


class Evaluator:
    """
    Extrinsic evaluator using ground-truth labelled SQL query benchmarks.

    For each dataset (Bombay / GooglePlus / UB):
      1. Embed each query via SIF-weighted token embeddings
      2. Remove first principal component (common discourse vector)
      3. L2-normalise
      4. Leave-one-out kNN (k=5, cosine similarity) → predicted label
      5. Compare with ground-truth labels → macro F1
    """

    SIF_A = 1e-3
    K = 5

    def __init__(self, evaluation_datasets_path='../evaluation_datasets'):
        self.bombay_df     = pd.read_csv(os.path.join(evaluation_datasets_path, 'bombay_queries.csv'),     sep='\t')
        self.googleplus_df = pd.read_csv(os.path.join(evaluation_datasets_path, 'googleplus_queries.csv'), sep='\t')
        self.ub_df         = pd.read_csv(os.path.join(evaluation_datasets_path, 'ub_queries.csv'),         sep='\t')
        print(f"📊 Loaded {len(self.bombay_df)} Bombay, "
              f"{len(self.googleplus_df)} GooglePlus, "
              f"{len(self.ub_df)} UB queries")
        self.pipeline = PreprocessingPipeline()
        self._cache = {'bombay': None, 'googleplus': None, 'ub': None}

    # ------------------------------------------------------------------ #
    @staticmethod
    def _remove_first_pc(X: np.ndarray) -> np.ndarray:
        X_centered = X - X.mean(axis=0)
        _, _, Vt = np.linalg.svd(X_centered, full_matrices=False)
        pc = Vt[0]
        return X - np.outer(X.dot(pc), pc)

    # ------------------------------------------------------------------ #
    def _infer(self, target: str, embedding_matrix: np.ndarray,
               vocab_dict: dict, word_freq: dict):
        """SIF embed + PC remove + L2 norm. Returns (X [N, D], valid_idx)."""
        df_map = {'bombay': self.bombay_df, 'googleplus': self.googleplus_df, 'ub': self.ub_df}
        df = df_map[target]

        if self._cache[target] is None:
            self._cache[target] = [
                self.pipeline.tokenize(q).split() for q in df['query'].tolist()
            ]
        token_lists = self._cache[target]

        a = self.SIF_A
        raw_embs, valid_idx = [], []
        for i, tokens in enumerate(token_lists):
            embs, weights = [], []
            for tok in tokens:
                if tok in vocab_dict:
                    embs.append(embedding_matrix[vocab_dict[tok]])
                    pw = word_freq.get(tok, 1e-8) if word_freq else 1.0
                    weights.append(a / (a + pw))
            if embs:
                w = np.array(weights, dtype=np.float32)
                raw_embs.append(np.average(embs, axis=0, weights=w))
                valid_idx.append(i)

        if not raw_embs:
            return np.zeros((0, embedding_matrix.shape[1]), dtype=np.float32), []

        X = np.stack(raw_embs).astype(np.float32)
        if X.shape[0] > 1:
            X = self._remove_first_pc(X)
        norms = np.linalg.norm(X, axis=1, keepdims=True)
        X = X / np.clip(norms, 1e-8, None)
        return X, valid_idx

    # ------------------------------------------------------------------ #
    @staticmethod
    def _knn_loo_predict(X: np.ndarray, labels: np.ndarray, k: int = 5) -> np.ndarray:
        """LOO kNN majority-vote using cosine similarity (X already L2-normed)."""
        unique_labels, encoded = np.unique(labels, return_inverse=True)
        sim = X @ X.T
        np.fill_diagonal(sim, -np.inf)
        k = min(k, X.shape[0] - 1)
        top_k = np.argpartition(sim, -k, axis=1)[:, -k:]
        preds = []
        for neighbors in top_k:
            counts = np.bincount(encoded[neighbors], minlength=len(unique_labels))
            preds.append(unique_labels[int(np.argmax(counts))])
        return np.array(preds)

    # ------------------------------------------------------------------ #
    def __call__(self, embedding_matrix: np.ndarray, vocab_dict: dict,
                 word_freq: dict = None) -> dict:
        """
        Evaluate on all three benchmarks.

        Returns dict with keys:
            bombay_f1, bombay_report,
            googleplus_f1, googleplus_report,
            ub_f1, ub_report
        """
        results = {}
        for target, df in [('bombay',     self.bombay_df),
                            ('googleplus', self.googleplus_df),
                            ('ub',         self.ub_df)]:
            X, valid_idx = self._infer(target, embedding_matrix, vocab_dict, word_freq)
            if X.shape[0] < 2:
                results[f'{target}_f1']     = 0.0
                results[f'{target}_report'] = ''
                continue

            true   = df['label'].values[valid_idx]
            pred   = self._knn_loo_predict(X, true, k=self.K)
            report = classification_report(true, pred, zero_division=0)

            macro_f1 = 0.0
            for line in report.strip().split('\n'):
                if 'macro avg' in line:
                    macro_f1 = float(line.split()[-2])
                    break

            results[f'{target}_f1']     = macro_f1
            results[f'{target}_report'] = report

        return results
