from gensim.models import Word2Vec
from gensim.models.callbacks import CallbackAny2Vec
import pandas as pd
import os
import pickle
import hashlib
import numpy as np
import json
import matplotlib.pyplot as plt
import logging

from sensate.pipeline.preprocessing.preprocessing_pipeline import PreprocessingPipeline
from sensate.pipeline.training.evaluating_pipeline import Evaluator


class _EpochCallback(CallbackAny2Vec):
    """Gensim callback: track loss and run evaluation every N epochs."""

    def __init__(self, trainer, eval_every_n_epochs: int = 1):
        self.trainer = trainer
        self.eval_every_n_epochs = eval_every_n_epochs
        self.epoch = 0

    def on_epoch_begin(self, model):
        self.epoch += 1
        model.running_training_loss = 0.0

    def on_epoch_end(self, model):
        loss = model.get_latest_training_loss()
        cfg = self.trainer.config.training

        if self.epoch % self.eval_every_n_epochs == 0 or self.epoch == cfg.num_epochs:
            result = self.trainer._evaluate_model()
            avg_f1 = result['avg_f1']
            print(f"  Epoch {self.epoch}/{cfg.num_epochs} — loss: {loss:.4f}  |  "
                  f"Bombay: {result['bombay_f1']:.4f}  GGPlus: {result['googleplus_f1']:.4f}  "
                  f"UB: {result['ub_f1']:.4f}  Avg: {avg_f1:.4f}")

            self.trainer.history['epoch'].append(self.epoch)
            self.trainer.history['train_loss'].append(loss)
            self.trainer.history['bombay_f1'].append(result['bombay_f1'])
            self.trainer.history['googleplus_f1'].append(result['googleplus_f1'])
            self.trainer.history['ub_f1'].append(result['ub_f1'])
            self.trainer.history['avg_f1'].append(avg_f1)

            if avg_f1 > self.trainer.best_avg_f1:
                self.trainer.best_avg_f1 = avg_f1
                self.trainer.best_epoch = self.epoch
                model.save(os.path.join(self.trainer.checkpoint_dir, 'w2v_best.model'))
        else:
            print(f"  Epoch {self.epoch}/{cfg.num_epochs} — loss: {loss:.4f}")
            self.trainer.history['epoch'].append(self.epoch)
            self.trainer.history['train_loss'].append(loss)
            self.trainer.history['bombay_f1'].append(0.0)
            self.trainer.history['googleplus_f1'].append(0.0)
            self.trainer.history['ub_f1'].append(0.0)
            self.trainer.history['avg_f1'].append(0.0)


class Trainer:
    def __init__(self, config=None):
        assert config is not None, "Config must be provided, the format is defined in sensate.schema"
        self.config = config
        self.preprocessing_pipeline = PreprocessingPipeline()
        self.evaluator = Evaluator()
        self.corpus = None
        self.vocab_table = None
        self.word_to_id = None
        self.model = None

        self.history = {
            'epoch': [],
            'train_loss': [],
            'bombay_f1': [],
            'googleplus_f1': [],
            'ub_f1': [],
            'avg_f1': [],
        }
        self.best_avg_f1 = -1.0
        self.best_epoch = -1
        self.checkpoint_dir = None

    # ------------------------------------------------------------------ #
    #  prepare()                                                           #
    # ------------------------------------------------------------------ #
    def prepare(self, data, cache_dir: str = '../cache'):
        """
        Preprocess data and build vocabulary.
        Caches the result to `cache_dir` so subsequent runs skip the
        expensive decomposition + preprocessing step.
        Cache is invalidated automatically if:
          - `len(data)` changes, OR
          - the preprocessing pipeline source code changes.
        """
        import inspect
        os.makedirs(cache_dir, exist_ok=True)
        cache_path = os.path.join(cache_dir, 'corpus_cache.pkl')
        input_count = len(data)

        # Fingerprint the preprocessing pipeline source so code changes
        # automatically bust the cache.
        pipeline_src = inspect.getsource(
            self.preprocessing_pipeline.__class__
        ).encode()
        pipeline_hash = hashlib.md5(pipeline_src).hexdigest()

        # ---- Try loading cache ---------------------------------------- #
        if os.path.exists(cache_path):
            print(f"🗂️  Found corpus cache at {cache_path} — checking...")
            try:
                with open(cache_path, 'rb') as f:
                    cached = pickle.load(f)
                stale_count = cached.get('input_count') != input_count
                stale_hash  = cached.get('pipeline_hash') != pipeline_hash
                if stale_count:
                    print(f"   ⚠️  Cache input_count={cached.get('input_count'):,} "
                          f"!= current {input_count:,} — rebuilding.")
                elif stale_hash:
                    print("   ⚠️  Preprocessing pipeline changed — rebuilding cache.")
                else:
                    self.corpus      = cached['corpus']
                    self.word_to_id  = cached['word_to_id']
                    self.vocab_table = cached['vocab_table']
                    print(f"   ✅ Cache loaded: {len(self.corpus):,} sentences, "
                          f"{len(self.vocab_table):,} vocab words.")
                    print(f"   Window size: {self.config.training.window_size}")
                    return
            except Exception as e:
                print(f"   ⚠️  Cache read failed ({e}) — rebuilding.")

        # ---- Build from scratch --------------------------------------- #
        num_workers = getattr(self.config.training, 'num_workers', 1)
        self.corpus = self.preprocessing_pipeline(data, num_workers=num_workers)

        print("📚 Building vocabulary...")
        unique_words = sorted(set(w for sent in self.corpus for w in sent))
        self.word_to_id = {w: i for i, w in enumerate(unique_words)}
        self.vocab_table = pd.DataFrame({'word': unique_words, 'id': list(range(len(unique_words)))})

        print(f"   Vocab size : {len(self.vocab_table):,}")
        print(f"   Sentences  : {len(self.corpus):,}")
        print(f"   Window size: {self.config.training.window_size}")
        print(f"Vocab Table:\n{self.vocab_table.head()}")

        # ---- Save cache ----------------------------------------------- #
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump({
                    'input_count':   input_count,
                    'pipeline_hash': pipeline_hash,
                    'corpus':        self.corpus,
                    'word_to_id':    self.word_to_id,
                    'vocab_table':   self.vocab_table,
                }, f, protocol=pickle.HIGHEST_PROTOCOL)
            print(f"💾 Corpus cache saved to {cache_path} (input_count={input_count:,})")
        except Exception as e:
            print(f"   ⚠️  Could not save cache: {e}")

    # ------------------------------------------------------------------ #
    #  helpers                                                             #
    # ------------------------------------------------------------------ #
    def _build_embedding_inputs(self):
        vocab_dict = dict(zip(self.vocab_table['word'], self.vocab_table['id']))
        n_vocab = len(self.vocab_table)
        embedding_dim = self.config.training.embedding_dim
        embedding_matrix = np.zeros((n_vocab, embedding_dim), dtype=np.float32)
        for word, wid in vocab_dict.items():
            if word in self.model.wv:
                embedding_matrix[wid] = self.model.wv[word]
        total_count = sum(self.model.wv.get_vecattr(w, 'count') for w in self.model.wv.key_to_index)
        word_freq = {
            word: self.model.wv.get_vecattr(word, 'count') / total_count
            for word in vocab_dict if word in self.model.wv
        }
        return embedding_matrix, vocab_dict, word_freq

    def _evaluate_model(self) -> dict:
        embedding_matrix, vocab_dict, word_freq = self._build_embedding_inputs()
        result = self.evaluator(embedding_matrix, vocab_dict, word_freq)
        avg_f1 = (result['bombay_f1'] + result['googleplus_f1'] + result['ub_f1']) / 3.0
        result['avg_f1'] = avg_f1
        return result

    def _plot_training_progress(self, save_path):
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle('Training Progress', fontsize=14, fontweight='bold')
        epochs = self.history['epoch']

        axes[0].plot(epochs, self.history['train_loss'], 'b-', linewidth=2, marker='o')
        axes[0].set_xlabel('Epoch')
        axes[0].set_ylabel('Loss')
        axes[0].set_title('Training Loss')
        axes[0].grid(True, alpha=0.3)

        axes[1].plot(epochs, self.history['bombay_f1'],     'r-',  linewidth=2, marker='s', label='Bombay')
        axes[1].plot(epochs, self.history['googleplus_f1'], 'b-',  linewidth=2, marker='^', label='GooglePlus')
        axes[1].plot(epochs, self.history['ub_f1'],         'g-',  linewidth=2, marker='D', label='UB')
        axes[1].plot(epochs, self.history['avg_f1'],        'k--', linewidth=2, marker='o', label='Avg')
        if self.best_epoch >= 0:
            axes[1].axvline(x=self.best_epoch, color='orange', linestyle='--', label=f'Best (Epoch {self.best_epoch})')
        axes[1].set_xlabel('Epoch')
        axes[1].set_ylabel('Macro F1')
        axes[1].set_title('kNN-LOO F1 (Eval)')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"📊 Training progress plot saved to {save_path}")
        plt.close()

    def _save_history(self, save_path):
        """Save training history to JSON."""
        with open(save_path, 'w') as f:
            json.dump(self.history, f, indent=2)
        print(f"📝 Training history saved to {save_path}")

    # ------------------------------------------------------------------ #
    #  fit()  — gensim Word2Vec                                           #
    # ------------------------------------------------------------------ #
    def fit(self, checkpoint_dir='checkpoints', eval_every_n_epochs=1):
        """
        Train a single-sense Word2Vec model using gensim.

        Args:
            checkpoint_dir: Directory to save checkpoints
            eval_every_n_epochs: Evaluate every N epochs (default: 1)
        """
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)

        cfg = self.config.training
        logging.getLogger('gensim').setLevel(logging.WARNING)

        n_sent = len(self.corpus)
        print("\n" + "=" * 60)
        print("🚀 Starting Single-Sense Word2Vec Training (gensim)")
        print(f"   Corpus: {n_sent:,} sentences  |  "
              f"Vocab (pre-filter): {len(self.vocab_table)}  |  "
              f"Epochs: {cfg.num_epochs}")
        print("=" * 60)

        self.model = Word2Vec(
            vector_size=cfg.embedding_dim,
            window=cfg.window_size,
            min_count=cfg.min_count,
            sg=cfg.sg,
            negative=cfg.negative,
            sample=cfg.sample,
            alpha=cfg.alpha,
            min_alpha=cfg.min_alpha,
            workers=cfg.num_workers,
            seed=cfg.seed,
            compute_loss=True,
        )
        self.model.build_vocab(self.corpus)
        print(f"   Effective vocab after min_count={cfg.min_count}: {len(self.model.wv)}")

        callback = _EpochCallback(trainer=self, eval_every_n_epochs=eval_every_n_epochs)
        self.model.train(
            self.corpus,
            total_examples=self.model.corpus_count,
            epochs=cfg.num_epochs,
            compute_loss=True,
            callbacks=[callback],
        )

        # Reload best if available
        best_path = os.path.join(checkpoint_dir, 'w2v_best.model')
        if os.path.exists(best_path) and self.best_epoch > 0:
            self.model = Word2Vec.load(best_path)
            print(f"\n✓ Reloaded best model from epoch {self.best_epoch}")

        print("\n" + "=" * 60)
        print("✅ Training Completed!")
        print(f"🏆 Best Avg F1: {self.best_avg_f1:.4f} at Epoch {self.best_epoch}")
        print("=" * 60 + "\n")

        plot_path = os.path.join(checkpoint_dir, 'training_progress.png')
        self._plot_training_progress(plot_path)

        history_path = os.path.join(checkpoint_dir, 'training_history.json')
        self._save_history(history_path)

    # ------------------------------------------------------------------ #
    #  save_model() — export to CSV / BIN (single-sense)                  #
    # ------------------------------------------------------------------ #
    def save_model(self, path=None, **_kwargs):
        """
        Export model embeddings to human-readable format (CSV) and binary.

        Args:
            path: Directory to save the model files
        """
        assert path is not None, "Path must be provided to save the model"
        os.makedirs(path, exist_ok=True)

        # ---- Vocabulary CSV ------------------------------------------ #
        vocab_path = os.path.join(path, 'vocab.csv')
        self.vocab_table.to_csv(vocab_path, index=False)
        print(f"✓ Vocabulary saved to {vocab_path}")
        print(f"  Total words: {len(self.vocab_table)}")

        # ---- Vocabulary BIN (for PostgreSQL) ------------------------- #
        vocab_bin_path = os.path.join(path, 'vocab.bin')
        with open(vocab_bin_path, 'wb') as f:
            f.write(len(self.vocab_table).to_bytes(4, byteorder='little'))
            for _, row in self.vocab_table.iterrows():
                word_bytes = row['word'].encode('utf-8')
                f.write(len(word_bytes).to_bytes(4, byteorder='little'))
                f.write(word_bytes)
                f.write(int(row['id']).to_bytes(4, byteorder='little'))
        print(f"✓ Vocabulary binary saved to {vocab_bin_path}")

        # ---- Embeddings CSV (single-sense: sense_id always 0) -------- #
        n_vocab = len(self.vocab_table)
        embedding_dim = self.config.training.embedding_dim
        word_id_to_word = dict(zip(self.vocab_table['id'], self.vocab_table['word']))

        records = []
        for word_id in range(n_vocab):
            word = word_id_to_word.get(word_id, f"<UNK_{word_id}>")
            if word in self.model.wv:
                embedding = self.model.wv[word].tolist()
            else:
                embedding = [0.0] * embedding_dim
            records.append({'word': word, 'embedding': embedding})

        sense_df = pd.DataFrame(records)
        csv_path = os.path.join(path, 'sense_embeddings.csv')
        sense_df.to_csv(csv_path, index=False)
        print(f"✓ Embeddings saved to {csv_path}")
        print(f"  Total records: {len(records)} ({n_vocab} words × 1 sense)")

        # ---- Embeddings BIN (for PostgreSQL) ------------------------- #
        bin_path = os.path.join(path, 'sense_embeddings.bin')
        with open(bin_path, 'wb') as f:
            f.write(len(records).to_bytes(4, byteorder='little'))
            f.write(embedding_dim.to_bytes(4, byteorder='little'))
            for record in records:
                word_bytes = record['word'].encode('utf-8')
                f.write(len(word_bytes).to_bytes(4, byteorder='little'))
                f.write(word_bytes)
                embedding_array = np.array(record['embedding'], dtype=np.float32)
                f.write(embedding_array.tobytes())
        print(f"✓ Embeddings binary saved to {bin_path}")
        print(f"  Binary file size: {os.path.getsize(bin_path) / (1024 * 1024):.2f} MB")

        # ---- Save native gensim model -------------------------------- #
        model_path = os.path.join(path, 'w2v_model.model')
        self.model.save(model_path)
        print(f"✓ Gensim model saved to {model_path}")

        # ---- Copy training history / plot if available --------------- #
        if self.checkpoint_dir and os.path.exists(self.checkpoint_dir):
            import shutil
            for fname in ('training_history.json', 'training_progress.png'):
                src = os.path.join(self.checkpoint_dir, fname)
                if os.path.exists(src):
                    shutil.copy(src, os.path.join(path, fname))
                    print(f"✓ {fname} copied to {path}")

        print(f"\n🎉 Model exported successfully to {path}")
        return csv_path