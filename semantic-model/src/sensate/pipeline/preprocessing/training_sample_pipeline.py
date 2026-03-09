import pandas as pd
from typing import List, Dict
from sensate.pipeline.preprocessing.corpus_pipeline import PairGenerator
from tqdm import tqdm
import numpy as np


class TrainingSampleGenerator:
    def __init__(self, window_size: int = 2):
        self.pair_generator = PairGenerator(window_size=window_size)
        self.next_id = 0
        self.word_id_counter = 0
        self.query_id_counter = 0

    def __call__(self, corpus: List[List[str]]) -> tuple:
        """
        3 tables:
        - vocab_table: DataFrame with columns [word, id]
        - query_table: DataFrame with columns [id, sql_query, sql_length]
        - base_table: DataFrame with columns [id, center_word_id, center_pos, context_word_id, sql_query_id]
        """
        # Step 1: Generate pairs (center-context)
        pairs = self.pair_generator(corpus)

        # Step 2: Build vocabulary
        print("📚 Building vocabulary...")
        unique_words = set(word for sentence in corpus for word in sentence)
        vocab_dict = {}
        for word in tqdm(unique_words, desc="Step 2: Building vocab", unit="word"):
            vocab_dict[word] = self.word_id_counter
            self.word_id_counter += 1

        # Step 3: Build query table (lightweight, just metadata)
        print("📝 Building query table...")
        query_data = {
            'id': list(range(len(corpus))),
            'sql_query': [" ".join(s) for s in tqdm(corpus, desc="Step 3: Building queries", unit="query")],
            'sql_length': [len(s) for s in corpus]
        }
        query_table = pd.DataFrame(query_data)
        self.query_id_counter = len(corpus)
        del query_data  # Free immediately

        # Step 4: Build base table
        print("🔢 Building base table...")
        
        # Build base table in chunks using numpy arrays
        chunk_size = 100000
        base_chunks = []
        
        # Temporary arrays for current chunk
        chunk_ids = np.empty(chunk_size, dtype=np.int32)
        chunk_center_word_ids = np.empty(chunk_size, dtype=np.int32)
        chunk_center_pos = np.empty(chunk_size, dtype=np.int32)
        chunk_context_word_ids = np.empty(chunk_size, dtype=np.int32)
        chunk_query_ids = np.empty(chunk_size, dtype=np.int32)
        chunk_idx = 0
        
        for query_id in tqdm(range(len(pairs)), desc="Step 4: Building base table", unit="query"):
            sentence_pairs = pairs[query_id]
            sentence = corpus[query_id]
            
            # Build base entries for this query using numpy arrays
            # sentence_pairs now contains (center_idx, context_idx) position tuples
            for center_idx, context_idx in sentence_pairs:
                center = sentence[center_idx]
                context = sentence[context_idx]
                if center not in vocab_dict or context not in vocab_dict:
                    continue
                chunk_ids[chunk_idx] = self.next_id
                chunk_center_word_ids[chunk_idx] = vocab_dict[center]
                chunk_center_pos[chunk_idx] = center_idx  # exact position, no overwrite issue
                chunk_context_word_ids[chunk_idx] = vocab_dict[context]
                chunk_query_ids[chunk_idx] = query_id
                
                chunk_idx += 1
                self.next_id += 1
                
                # When chunk is full, convert to DataFrame and reset
                if chunk_idx >= chunk_size:
                    base_chunks.append(pd.DataFrame({
                        'id': chunk_ids.copy(),
                        'center_word_id': chunk_center_word_ids.copy(),
                        'center_pos': chunk_center_pos.copy(),
                        'context_word_id': chunk_context_word_ids.copy(),
                        'sql_query_id': chunk_query_ids.copy()
                    }))
                    chunk_idx = 0
            
            # Free memory for this query immediately
            pairs[query_id] = None
        
        # Clear large structures
        del pairs
        
        # Add remaining entries
        if chunk_idx > 0:
            base_chunks.append(pd.DataFrame({
                'id': chunk_ids[:chunk_idx].copy(),
                'center_word_id': chunk_center_word_ids[:chunk_idx].copy(),
                'center_pos': chunk_center_pos[:chunk_idx].copy(),
                'context_word_id': chunk_context_word_ids[:chunk_idx].copy(),
                'sql_query_id': chunk_query_ids[:chunk_idx].copy()
            }))
        
        # Free chunk arrays
        del chunk_ids, chunk_center_word_ids, chunk_center_pos, chunk_context_word_ids, chunk_query_ids
        
        # Concatenate all chunks efficiently
        print(f"   ✓ Concatenating {len(base_chunks)} chunks...")
        base_table = pd.concat(base_chunks, ignore_index=True) if base_chunks else pd.DataFrame()
        del base_chunks

        # Convert to pandas DataFrames
        vocab_table = pd.DataFrame(list(vocab_dict.items()), columns=['word', 'id'])

        return vocab_table, query_table, base_table
