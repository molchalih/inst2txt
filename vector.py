from transformers import AutoTokenizer, AutoModel
from torch.nn.functional import cosine_similarity
import torch
import torch.nn.functional as F
import numpy as np
from db_manager import InstagramDataManager

#Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

def generate_embeddings_for_reels():
    """Generate embeddings for all reels with descriptions"""
    print("=== Starting Embedding Generation ===")
    
    # Initialize database manager
    db_manager = InstagramDataManager()
    
    # Ensure embeddings column exists
    db_manager.ensure_embeddings_column()
    
    # Load model from HuggingFace Hub
    print("Loading sentence transformer model...")
    tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/all-mpnet-base-v2')
    model = AutoModel.from_pretrained('sentence-transformers/all-mpnet-base-v2')
    
    # Get reels that need embedding generation
    reels = db_manager.get_reels_for_embedding_generation()
    
    if not reels:
        print("No reels found needing embedding generation")
        return
    
    print(f"Found {len(reels)} reels needing embedding generation")
    
    processed_count = 0
    
    for pk, description in reels:
        if not description:
            continue
        
        try:
            # Tokenize sentence
            encoded_input = tokenizer([description], padding=True, truncation=True, return_tensors='pt')
            
            # Compute token embeddings
            with torch.no_grad():
                model_output = model(**encoded_input)
            
            # Perform pooling
            sentence_embedding = mean_pooling(model_output, encoded_input['attention_mask'])
            
            # Normalize embeddings
            sentence_embedding = F.normalize(sentence_embedding, p=2, dim=1)
            
            # Convert to numpy array and save as blob
            embedding_array = sentence_embedding.cpu().numpy().astype(np.float32)
            embedding_blob = embedding_array.tobytes()
            
            # Save to database
            db_manager.save_embedding(pk, embedding_blob)
            processed_count += 1
            
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} reels...")
            
        except Exception as e:
            print(f"Error processing reel {pk}: {e}")
            continue
    
    print("\n=== Embedding Generation Complete ===")
    print(f"Generated embeddings for {processed_count} reels")
    print(f"Total processed: {len(reels)}")

if __name__ == "__main__":
    generate_embeddings_for_reels()