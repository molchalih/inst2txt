import av
import torch
import numpy as np
import os
import time
import shutil
from huggingface_hub import snapshot_download
from scenedetect import detect, ContentDetector
from transformers import LlavaNextVideoProcessor, LlavaNextVideoForConditionalGeneration
from transformers.utils.quantization_config import BitsAndBytesConfig
from db_manager import InstagramDataManager

# Global variables to hold the model and processor, initialized to None
model = None
processor = None

def load_video_model():
    """
    Loads the video model and processor if they haven't been loaded yet.
    Ensures model and processor are downloaded if not available locally.
    This function uses global variables to ensure the model is only loaded once.
    """
    global model, processor
    if model is None or processor is None:
        print("Loading model and processor...")
        start_time = time.time()
        
        model_id = "llava-hf/LLaVA-NeXT-Video-7B-DPO-hf"
        model_cache_dir = "./model_cache"
        processor_path = "./llava-processor"

        # Step 1: Download the entire model snapshot if it's not already cached.
        # This gives us the local path to all model and processor files.
        print(f"Ensuring model snapshot for {model_id} is available at {model_cache_dir}...")
        snapshot_path = snapshot_download(
            repo_id=model_id,
            cache_dir=model_cache_dir
        )
        print(f"Snapshot is at: {snapshot_path}")
        
        # Step 2: If the local processor directory doesn't exist, create it by copying
        # the necessary files from the downloaded snapshot.
        if not os.path.isdir(processor_path):
            print(f"Processor directory not found at {processor_path}. Copying files from snapshot...")
            os.makedirs(processor_path, exist_ok=True)
            
            processor_files = [
                "added_tokens.json", "chat_template.json", "preprocessor_config.json",
                "processor_config.json", "special_tokens_map.json", "tokenizer.json",
                "tokenizer.model", "tokenizer_config.json"
            ]
            
            for filename in processor_files:
                source_file = os.path.join(snapshot_path, filename)
                if os.path.exists(source_file):
                    shutil.copy(source_file, processor_path)
            
            print(f"Processor files copied to {processor_path}")

        # Step 3: Load the model from the local snapshot path.
        print(f"Loading model from local path: {snapshot_path}...")
        model = LlavaNextVideoForConditionalGeneration.from_pretrained(
            snapshot_path,  # Load from the local directory
            torch_dtype=torch.bfloat16,
            low_cpu_mem_usage=True,
            quantization_config=BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_use_double_quant=True,
            ),
            device_map="auto",
            attn_implementation="flash_attention_2",
        )
        print("Model loaded.")

        # Step 4: Load the processor from our clean, local processor directory.
        processor = LlavaNextVideoProcessor.from_pretrained(processor_path, use_fast=True)
        print("Processor loaded.")
        
        total_load_time = time.time() - start_time
        print(f"Model and processor loaded in {total_load_time:.2f} seconds")

# === Database functions ===
def build_prompt_from_reel_info(reel_info):
    """Build prompt from reel information"""
    # Strictly prefer short versions of content if available, otherwise use the full version.
    caption = reel_info.get('caption_english_short') or reel_info.get('caption_english') or reel_info.get('caption')
    audio_content = reel_info.get('audio_content_short') or reel_info.get('audio_content')
    audio_type = reel_info.get('audio_type')

    # Log which versions are being used for clarity
    if reel_info.get('caption_english_short'):
        print("Using concise caption for context.")
    elif reel_info.get('caption_english'):
        print("Using full translated caption for context.")
    elif reel_info.get('caption'):
        print("Using original caption for context.")
    else:
        print("No caption available for context.")

    if reel_info.get('audio_content_short'):
        print("Using concise audio transcription for context.")
    elif reel_info.get('audio_content'):
        print("Using full audio transcription for context.")
    else:
        print("No audio content available for context.")
    
    if audio_type:
        print(f"Audio type: {audio_type}")

    # Define the core task for the model, which will always be included
    core_task = (
        "You are describing the aesthetic of a video. Answer all questions in 80 words max."
        "How would you categorize the aesthetic of this video? Mention visual style, emotional tone and narrative. "
        "Which kind of social media community or aesthetic subculture this content likely belongs to? "
        "What this video suggests about the author's role? "
        "What short aesthetic labels can be used to tag this video? (don't mention the label in your response, just list the tags)."
    )

    # Build context string from available data
    context_parts = []
    if caption:
        context_parts.append(f"Author's caption: {caption}")

    if audio_content and audio_type:
        if audio_type == 'speech':
            context_parts.append(f"Spoken speech: {audio_content}")
        elif audio_type == 'music':
            context_parts.append(f"Music: {audio_content}")
        else:
            context_parts.append(f"Audio ({audio_type}): {audio_content}")

    # Combine context with the core task
    if context_parts:
        context_header = "This video includes the following context. Use it to inform your response.\n"
        context_string = "\n".join(context_parts)
        combined_prompt = f"{context_header}{context_string}\n\n{core_task}"
    else:
        # No context available, just give the core task
        combined_prompt = core_task
    
    return combined_prompt

# === Scene-based frame sampling ===
def get_scene_indices(video_path, fps, max_frames=28):
    """
    Get frame indices based on scene detection, respecting maximum frame limit.
    
    Args:
        video_path: Path to video file
        fps: Frames per second of the video
        max_frames: Maximum number of frames to return (default 16 for LLaVA)
    
    Returns:
        List of frame indices to sample
    """
    scenes = detect(video_path, ContentDetector(threshold=32.0))
    indices = []
    
    # Get video duration
    container = av.open(video_path)
    total_frames = container.streams.video[0].frames
    fps_float = float(fps)  # Convert Fraction to float
    total_duration = total_frames / fps_float
    container.close()
    
    # Adaptive frame limit based on video duration
    if total_duration <= 15:  # Short videos
        max_frames = min(6, max_frames)
    elif total_duration <= 30:  # Medium videos
        max_frames = min(10, max_frames)
    elif total_duration <= 60:  # Long videos
        max_frames = min(16, max_frames)
    elif total_duration <= 120:  # Very long videos
        max_frames = min(20, max_frames)
    else:  # Very long videos
        max_frames = min(24, max_frames)
    
    print(f"Video duration: {total_duration:.1f}s, using max {max_frames} frames")
    
    if not scenes:
        # Fallback: sample evenly if no scenes detected
        step = max(1, total_frames // max_frames)
        indices = list(range(0, total_frames, step))[:max_frames]
        return indices
    
    print(f"Detected {len(scenes)} scenes")
    
    # Calculate adaptive frames per scene based on scene count
    if len(scenes) <= 3:
        frames_per_scene = max(2, max_frames // len(scenes))
    elif len(scenes) <= 6:
        frames_per_scene = max(1, max_frames // len(scenes))
    else:
        # Too many scenes, be very selective and spread across timeline
        frames_per_scene = 1
        max_frames = min(max_frames, len(scenes))
    
    print(f"Using {frames_per_scene} frames per scene")
    
    # Sample frames from each scene
    if len(scenes) <= max_frames:
        # We can sample from all scenes
        for i, (start_time, end_time) in enumerate(scenes):
            start_sec = start_time.get_seconds()
            end_sec = end_time.get_seconds()
            scene_duration = end_sec - start_sec
            
            # Always take the first frame of the scene
            indices.append(int(start_sec * fps_float))
            
            # Add additional frames if scene is long enough and we have quota
            if frames_per_scene > 1 and scene_duration > 2.0:
                # Take middle frame
                mid_time = start_sec + (scene_duration / 2)
                indices.append(int(mid_time * fps_float))
                
                # Take last frame if scene is very long
                if frames_per_scene > 2 and scene_duration > 5.0:
                    last_time = end_sec - 0.5  # 0.5 seconds before end
                    indices.append(int(last_time * fps_float))
    else:
        # Too many scenes, sample evenly across timeline
        print(f"Too many scenes ({len(scenes)}), sampling evenly across timeline")
        
        # Calculate target timestamps evenly distributed across video
        target_timestamps = []
        for i in range(max_frames):
            timestamp = (total_duration * i) / (max_frames - 1) if max_frames > 1 else total_duration / 2
            target_timestamps.append(timestamp)
        
        # Find the scene that contains each target timestamp
        for target_time in target_timestamps:
            for start_time, end_time in scenes:
                start_sec = start_time.get_seconds()
                end_sec = end_time.get_seconds()
                
                if start_sec <= target_time <= end_sec:
                    # Take frame closest to target timestamp
                    frame_time = target_time
                    indices.append(int(frame_time * fps_float))
                    break
            else:
                # If no scene contains this timestamp, take the closest scene start
                closest_scene = min(scenes, key=lambda x: abs(x[0].get_seconds() - target_time))
                indices.append(int(closest_scene[0].get_seconds() * fps_float))
    
    # Ensure we don't exceed max_frames and frames are within video bounds
    indices = sorted(set(indices))
    indices = [idx for idx in indices if idx < total_frames]
    
    # If we still have too many frames, sample evenly
    if len(indices) > max_frames:
        step = len(indices) // max_frames
        indices = indices[::step][:max_frames]
    
    print(f"Final frame count: {len(indices)}")
    
    # Show timecodes of selected frames
    timecodes = [idx / fps_float for idx in indices]
    print(f"Selected timecodes: {[f'{t:.1f}s' for t in timecodes]}")
    
    return indices[:max_frames]

def read_video_pyav(container, indices):
    frames = []
    container.seek(0)
    indices_set = set(indices)
    for i, frame in enumerate(container.decode(video=0)):
        if i > max(indices):
            break
        if i in indices_set:
            frames.append(frame.to_ndarray(format="rgb24"))
    return np.stack(frames)

# === Main function ===
def process_single_reel(reel_id, db_manager):
    """Process a single reel by ID"""
    # Ensure the model is loaded before processing
    load_video_model()

    # Add assertions to satisfy the linter and ensure the model is loaded.
    assert model is not None, "Model could not be loaded"
    assert processor is not None, "Processor could not be loaded"

    print(f"\nProcessing reel: {reel_id}")
    
    # Get reel information from database
    reel_info = db_manager.get_reel_info(reel_id)
    
    if not reel_info:
        print(f"Reel {reel_id} not found in database, skipping...")
        return False
    
    print(f"Found reel: {reel_info['code']}")
    
    # Check if video file exists
    video_path = f"data/reels/{reel_id}.mp4"
    if not os.path.exists(video_path):
        print(f"Video file not found: {video_path}, skipping...")
        return False
    
    # Build prompt from reel information
    print("=== Content being fed to model ===")
    combined_context = build_prompt_from_reel_info(reel_info)
    print("\n=== Final prompt ===")
    print(combined_context)
    
    # Process video
    video_start_time = time.time()
    container = av.open(video_path)
    fps = container.streams.video[0].average_rate
    frame_indices = get_scene_indices(video_path, fps)
    clip = read_video_pyav(container, frame_indices)
    container.close()
    video_time = time.time() - video_start_time
    print(f"\nVideo processing: {video_time:.3f} seconds")
    print(f"Processing {len(frame_indices)} frames")
    
    # Build conversation and prepare inputs
    prep_start_time = time.time()
    conversation = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": combined_context},
                {"type": "video", "path": video_path},
            ],
        },
    ]
    
    # Generate prompt and process
    prompt = processor.tokenizer.apply_chat_template(conversation, add_generation_prompt=True, tokenize=False)  # type: ignore
    inputs_video = processor(text=prompt, videos=clip, padding=True)  # type: ignore
    inputs_video = {k: v.to(model.device) for k, v in inputs_video.items()}
    prep_time = time.time() - prep_start_time
    print(f"Input preparation: {prep_time:.3f} seconds")
    
    # Run model
    inference_start_time = time.time()
    output = model.generate(**inputs_video, max_new_tokens=256, do_sample=False)
    inference_time = time.time() - inference_start_time
    print(f"Model inference: {inference_time:.3f} seconds")
    
    result = processor.decode(output[0][2:], skip_special_tokens=True).split("ASSISTANT:")[-1].strip()  # type: ignore
    
    print("\n=== Model Result ===")
    print(result)
    
    # Save result to database
    try:
        db_manager.set_model_description(reel_id, result)
        print(f"Successfully saved description for reel {reel_id}")
        return True
    except Exception as e:
        print(f"Error saving description for reel {reel_id}: {e}")
        return False

def main():
    """Process all selected reels from the database"""
    print("=== Starting Video Processing ===")
    
    # Initialize database manager
    db_manager = InstagramDataManager()
    
    # Get selected reels list
    selected_reels = db_manager.get_selected_reels_list()
    if not selected_reels:
        print("No selected reels found in database")
        return
    
    print(f"Found {len(selected_reels)} selected reels")
    
    # Filter out reels that already have descriptions
    reels_to_process = db_manager.get_reels_without_description(selected_reels)
    if not reels_to_process:
        print("All selected reels already have descriptions")
        return
    
    print(f"Processing {len(reels_to_process)} reels without descriptions")
    
    # Process each reel
    successful = 0
    failed = 0
    
    for i, reel_id in enumerate(reels_to_process, 1):
        print(f"\n=== Processing reel {i}/{len(reels_to_process)} ===")
        try:
            if process_single_reel(reel_id, db_manager):
                successful += 1
            else:
                failed += 1
        except Exception as e:
            print(f"Error processing reel {reel_id}: {e}")
            failed += 1
            continue
    
    print("\n=== Processing Complete ===")
    print(f"Successfully processed: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(reels_to_process)}")

# === Script entry point ===
if __name__ == "__main__":
    main()
