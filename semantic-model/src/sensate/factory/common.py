import torch

def get_GPU():
    # Optional: Print GPU info if available
    if torch.cuda.is_available():
        print(f"🚀 GPU: {torch.cuda.get_device_name(0)}")
        print(f"💾 CUDA memory: {torch.cuda.get_device_properties(0).total_memory // (1024**3)} GB")
    else:
        print("⚠️  CUDA not available, using CPU")
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    return device

device = get_GPU()