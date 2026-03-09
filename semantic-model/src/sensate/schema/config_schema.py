from pydantic import BaseModel

class TrainerConfig(BaseModel):
    window_size: int
    embedding_dim: int
    num_epochs: int
    num_workers: int

    # Gensim Word2Vec parameters
    min_count: int = 1
    sg: int = 1              # 1 = Skip-gram, 0 = CBOW
    negative: int = 5        # Number of negative samples
    sample: float = 0.001    # Subsampling threshold
    alpha: float = 0.025     # Initial learning rate
    min_alpha: float = 0.0001  # Final learning rate
    seed: int = 42
    
class GlobalConfigSchema(BaseModel):
    training: TrainerConfig
    # model_config: 
    
class BaseTableEntry(BaseModel):
    id: int
    center_word_id: int
    context_word_id: int
    sql_query_id: int
