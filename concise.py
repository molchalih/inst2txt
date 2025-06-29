import transformers
import torch
from huggingface_hub import snapshot_download
import sqlite3
import json
import os
from dotenv import load_dotenv
from transformers.utils.quantization_config import BitsAndBytesConfig
from db_manager import InstagramDataManager

# Load environment variables
load_dotenv()

# Download model if needed (idempotent)
snapshot_download(
    repo_id="meta-llama/Llama-3.1-8B-Instruct",
    local_dir="./Llama-3.1-8B-Instruct",
    ignore_patterns="original/consolidated.00.pth"
)

model_id = "./Llama-3.1-8B-Instruct"

# Singleton for pipeline
def get_pipeline():
    if not hasattr(get_pipeline, "_pipeline"):
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_use_double_quant=True,
        )
        get_pipeline._pipeline = transformers.pipeline(
            "text-generation",
            model=model_id,
            model_kwargs={
                "torch_dtype": torch.bfloat16,
                "attn_implementation": "flash_attention_2",
                "quantization_config": quantization_config,
            },
            device_map="auto"
        )
    return get_pipeline._pipeline

def shorten_text(text: str) -> str:
    """Shorten the provided text to max words using the Llama-3.1-8B-Instruct model."""
    max_words = os.getenv("POLICY_CONCISE_MAX_WORDS", "60")
    messages = [
        {"role": "system", "content": f"You are a chatbot who shortens the provided text (max {max_words} words). Output only the shortened text."},
        {"role": "user", "content": f"{text}"},
    ]
    pipeline = get_pipeline()
    outputs = pipeline(
        messages,
        max_new_tokens=256,
    )
    # The output from the pipeline is a list containing a dictionary.
    # The 'generated_text' key in the dictionary contains the full conversation history.
    # The assistant's response is the last message in that list.
    full_conversation = outputs[0]["generated_text"]
    if isinstance(full_conversation, list) and full_conversation:
        last_message = full_conversation[-1]
        if isinstance(last_message, dict) and last_message.get("role") == "assistant":
            return last_message.get("content", "").strip()

    # Fallback for unexpected formats
    return ""

def main():
    max_words_policy = int(os.getenv("POLICY_CONCISE_MAX_WORDS", "60"))
    data_manager = InstagramDataManager()

    # 1. Get all selected reel PKs from all users
    selected_pks = data_manager.get_selected_reels_list()
    if not selected_pks:
        print("No selected reels found to process.")
        return

    print(f"Found {len(selected_pks)} unique selected reels to process.")

    # 2. Get the data for these reels in one go
    conn = sqlite3.connect(data_manager.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    placeholders = ','.join('?' for _ in selected_pks)
    query = f"""
        SELECT pk, audio_content, audio_type, caption_english, audio_content_short, caption_english_short
        FROM reels
        WHERE pk IN ({placeholders})
    """
    cur.execute(query, selected_pks)
    reels_to_process = cur.fetchall()

    processed_audio = 0
    processed_caption = 0

    # 3. Iterate and process
    for reel in reels_to_process:
        pk = reel['pk']

        # Process audio_content
        audio_content = reel['audio_content']
        audio_type = reel['audio_type']
        audio_content_short = reel['audio_content_short']

        if audio_type == 'speech' and audio_content and not (audio_content_short and audio_content_short.strip()):
            if len(audio_content.split()) > max_words_policy:
                print(f"  Shortening audio_content for reel {pk} ({len(audio_content.split())} words)...")
                short_audio = shorten_text(audio_content)
                cur.execute("UPDATE reels SET audio_content_short = ? WHERE pk = ?", (short_audio, pk))
                processed_audio += 1
                conn.commit()

        # Process caption_english
        caption_english = reel['caption_english']
        caption_english_short = reel['caption_english_short']

        if caption_english and not (caption_english_short and caption_english_short.strip()):
             if len(caption_english.split()) > max_words_policy:
                print(f"  Shortening caption_english for reel {pk} ({len(caption_english.split())} words)...")
                short_caption = shorten_text(caption_english)
                cur.execute("UPDATE reels SET caption_english_short = ? WHERE pk = ?", (short_caption, pk))
                processed_caption += 1
                conn.commit()

    print(f"Done. Shortened {processed_audio} audio_content and {processed_caption} caption_english fields.")
    conn.close()

if __name__ == "__main__":
    main()