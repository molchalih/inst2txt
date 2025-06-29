import transformers
import torch
from huggingface_hub import snapshot_download
import sqlite3
import os
from dotenv import load_dotenv
from transformers.utils.quantization_config import BitsAndBytesConfig

# --- Basic Setup ---
load_dotenv()

DB_PATH = "data/instagram_database_primary.db"
MODEL_ID = "./Llama-3.1-8B-Instruct"

# --- Model Loading ---
def get_pipeline():
    """Singleton for the text-generation pipeline."""
    if not hasattr(get_pipeline, "_pipeline"):
        snapshot_download(
            repo_id="meta-llama/Llama-3.1-8B-Instruct",
            local_dir=MODEL_ID,
            ignore_patterns="original/consolidated.00.pth"
        )
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True,
        )
        get_pipeline._pipeline = transformers.pipeline(
            "text-generation",
            model=MODEL_ID,
            model_kwargs={
                "torch_dtype": torch.bfloat16,
                "attn_implementation": "flash_attention_2",
                "quantization_config": quantization_config,
            },
            device_map="auto"
        )
    return get_pipeline._pipeline

def shorten_text(text: str) -> str:
    """Shortens text using the Llama model."""
    max_words = os.getenv("POLICY_CONCISE_MAX_WORDS", "60")
    messages = [
        {"role": "system", "content": f"You are a chatbot who shortens the provided text (max {max_words} words). Output only the shortened text."},
        {"role": "user", "content": f"{text}"},
    ]
    pipeline = get_pipeline()
    outputs = pipeline(messages, max_new_tokens=256)
    full_conversation = outputs[0]["generated_text"]
    if isinstance(full_conversation, list) and full_conversation:
        last_message = full_conversation[-1]
        if isinstance(last_message, dict) and last_message.get("role") == "assistant":
            return last_message.get("content", "").strip()
    return ""

def main():
    """Main function to process and shorten audio content."""
    max_words_policy = int(os.getenv("POLICY_CONCISE_MAX_WORDS", "60"))

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get all reels with audio_type 'speech' and non-empty audio_content
    cur.execute("""
        SELECT pk, audio_content FROM reels
        WHERE audio_type = 'speech' AND audio_content IS NOT NULL AND audio_content != ''
    """)
    reels_to_process = cur.fetchall()

    print(f"Found {len(reels_to_process)} reels with 'speech' audio to process.")
    processed_count = 0

    for reel in reels_to_process:
        pk = reel['pk']
        audio_content = reel['audio_content']

        if len(audio_content.split()) > max_words_policy:
            print(f"  Shortening audio_content for reel {pk} ({len(audio_content.split())} words)...")
            short_audio = shorten_text(audio_content)
            cur.execute("UPDATE reels SET audio_content_short = ? WHERE pk = ?", (short_audio, pk))
            conn.commit()
            processed_count += 1
        else:
            # If the content is already short enough, just copy it over.
            print(f"  Audio for reel {pk} is already short enough. Copying to _short field.")
            cur.execute("UPDATE reels SET audio_content_short = ? WHERE pk = ?", (audio_content, pk))
            conn.commit()


    print(f"\nDone. Processed {processed_count} audio_content fields.")
    conn.close()

if __name__ == "__main__":
    main()
