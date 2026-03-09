"""
Binary file loader for vocabulary and sense embeddings.
These loaders can be used to read the .bin files and import into PostgreSQL.
"""

import numpy as np
import struct


def load_vocab_bin(file_path):
    """
    Load vocabulary from binary file.
    
    Binary format:
    - 4 bytes: number of records (int32, little-endian)
    - For each record:
        - 4 bytes: word length (int32, little-endian)
        - N bytes: word (UTF-8 encoded string)
        - 4 bytes: word id (int32, little-endian)
    
    Args:
        file_path: Path to the vocab.bin file
        
    Returns:
        List of tuples: [(word, word_id), ...]
    """
    vocab = []
    
    with open(file_path, 'rb') as f:
        # Read number of records
        num_records = int.from_bytes(f.read(4), byteorder='little')
        
        for _ in range(num_records):
            # Read word length
            word_length = int.from_bytes(f.read(4), byteorder='little')
            
            # Read word
            word = f.read(word_length).decode('utf-8')
            
            # Read word id
            word_id = int.from_bytes(f.read(4), byteorder='little')
            
            vocab.append((word, word_id))
    
    return vocab


def load_sense_embeddings_bin(file_path):
    """
    Load sense embeddings from binary file.
    
    Binary format:
    - 4 bytes: number of records (int32, little-endian)
    - 4 bytes: embedding dimension (int32, little-endian)
    - For each record:
        - 4 bytes: word length (int32, little-endian)
        - N bytes: word (UTF-8 encoded string)
        - 4 bytes: sense_id (int32, little-endian)
        - D*4 bytes: embedding (float32 array, little-endian)
    
    Args:
        file_path: Path to the sense_embeddings.bin file
        
    Returns:
        Tuple of (metadata, embeddings):
        - metadata: dict with 'num_records' and 'embedding_dim'
        - embeddings: List of tuples: [(word, sense_id, embedding_array), ...]
    """
    embeddings = []
    
    with open(file_path, 'rb') as f:
        # Read metadata
        num_records = int.from_bytes(f.read(4), byteorder='little')
        embedding_dim = int.from_bytes(f.read(4), byteorder='little')
        
        metadata = {
            'num_records': num_records,
            'embedding_dim': embedding_dim
        }
        
        for _ in range(num_records):
            # Read word length
            word_length = int.from_bytes(f.read(4), byteorder='little')
            
            # Read word
            word = f.read(word_length).decode('utf-8')
            
            # Read sense_id
            sense_id = int.from_bytes(f.read(4), byteorder='little')
            
            # Read embedding
            embedding_bytes = f.read(embedding_dim * 4)  # 4 bytes per float32
            embedding = np.frombuffer(embedding_bytes, dtype=np.float32)
            
            embeddings.append((word, sense_id, embedding))
    
    return metadata, embeddings


def load_sense_embeddings_bin_streaming(file_path, batch_size=1000):
    """
    Stream load sense embeddings from binary file in batches.
    Useful for large files that don't fit in memory.
    
    Args:
        file_path: Path to the sense_embeddings.bin file
        batch_size: Number of records to yield per batch
        
    Yields:
        Batches of embeddings: [(word, sense_id, embedding_array), ...]
    """
    with open(file_path, 'rb') as f:
        # Read metadata
        num_records = int.from_bytes(f.read(4), byteorder='little')
        embedding_dim = int.from_bytes(f.read(4), byteorder='little')
        
        batch = []
        for i in range(num_records):
            # Read word length
            word_length = int.from_bytes(f.read(4), byteorder='little')
            
            # Read word
            word = f.read(word_length).decode('utf-8')
            
            # Read sense_id
            sense_id = int.from_bytes(f.read(4), byteorder='little')
            
            # Read embedding
            embedding_bytes = f.read(embedding_dim * 4)
            embedding = np.frombuffer(embedding_bytes, dtype=np.float32)
            
            batch.append((word, sense_id, embedding))
            
            # Yield batch when full
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield remaining records
        if batch:
            yield batch


# Example usage for PostgreSQL import
def example_postgres_import():
    """
    Example of how to import binary files into PostgreSQL using psycopg2.
    
    This is just a template - you'll need to adapt it to your specific schema.
    """
    import psycopg2
    from psycopg2.extras import execute_batch
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="your_database",
        user="your_user",
        password="your_password",
        host="localhost"
    )
    cur = conn.cursor()
    
    # Create tables (example schema)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vocab (
            word TEXT PRIMARY KEY,
            word_id INTEGER NOT NULL
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sense_embeddings (
            word TEXT NOT NULL,
            sense_id INTEGER NOT NULL,
            embedding FLOAT4[] NOT NULL,
            PRIMARY KEY (word, sense_id)
        );
    """)
    
    # Load and insert vocabulary
    vocab_data = load_vocab_bin('../output/model/vocab.bin')
    execute_batch(
        cur,
        "INSERT INTO vocab (word, word_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        vocab_data
    )
    print(f"✓ Inserted {len(vocab_data)} vocabulary entries")
    
    # Load and insert sense embeddings in batches
    total_inserted = 0
    for batch in load_sense_embeddings_bin_streaming('../output/model/sense_embeddings.bin', batch_size=1000):
        # Convert numpy arrays to lists for PostgreSQL
        batch_data = [(word, sense_id, embedding.tolist()) for word, sense_id, embedding in batch]
        execute_batch(
            cur,
            "INSERT INTO sense_embeddings (word, sense_id, embedding) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            batch_data
        )
        total_inserted += len(batch)
        print(f"  Inserted {total_inserted} embeddings...")
    
    print(f"✓ Total inserted: {total_inserted} sense embeddings")
    
    # Commit and close
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ PostgreSQL import completed!")


if __name__ == "__main__":
    # Test loading
    print("Testing binary loaders...")
    
    # Test vocab loader
    try:
        vocab = load_vocab_bin('../output/model/vocab.bin')
        print(f"✓ Loaded {len(vocab)} vocabulary entries")
        print(f"  Sample: {vocab[:3]}")
    except FileNotFoundError:
        print("⚠️  vocab.bin not found - run training first")
    
    # Test embeddings loader
    try:
        metadata, embeddings = load_sense_embeddings_bin('../output/model/sense_embeddings.bin')
        print(f"✓ Loaded {len(embeddings)} sense embeddings")
        print(f"  Metadata: {metadata}")
        print(f"  Sample: word={embeddings[0][0]}, sense_id={embeddings[0][1]}, embedding_shape={embeddings[0][2].shape}")
    except FileNotFoundError:
        print("⚠️  sense_embeddings.bin not found - run training first")
    
    print("\nTo import into PostgreSQL, run: example_postgres_import()")
