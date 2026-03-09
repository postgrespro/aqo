import os
from pathlib import Path
from sensate.pipeline import Trainer
from utils import load_data, load_config

# Change working directory to src/ so relative paths work correctly
os.chdir(Path(__file__).parent)

if __name__ == "__main__":
    print("🚀 Starting Word2Vec training")

    # Initialize trainer
    config = load_config("config.yaml")
    trainer = Trainer(config=config)

    # Load dataset with HuggingFace authentication
    print("📊 Loading dataset...")
    data = load_data("viethq1906", "generated_sql_samples_from_benchmarks")

    # Start training
    print("🔥 Starting training...")
    trainer.prepare(data, cache_dir='../cache')
    trainer.fit()
    trainer.save_model("../output/model")

    print("✅ Training completed!")