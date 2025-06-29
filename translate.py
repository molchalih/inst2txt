from langdetect import detect
import argostranslate.package, argostranslate.translate
import time
from db_manager import InstagramDataManager
import re

def clean_text(text: str) -> str:
    """Removes emojis, newlines, and other unnecessary characters from text."""
    if not text:
        return ""
    # Remove newlines and carriage returns, replacing them with a space
    text = text.replace('\\n', ' ').replace('\\r', ' ')
    
    # Regex to remove emojis and many other symbols.
    try:
        # UCS-4
        emoji_pattern = re.compile(u'['
                                   u'\U0001F300-\U0001F5FF'  # symbols & pictographs
                                   u'\U0001F600-\U0001F64F'  # emoticons
                                   u'\U0001F680-\U0001F6FF'  # transport & map symbols
                                   u'\U0001F1E0-\U0001F1FF'  # flags (iOS)
                                   u'\U00002702-\U000027B0'
                                   u'\U000024C2-\U0001F251'
                                   ']+', flags=re.UNICODE)
    except re.error:
        # UCS-2
        emoji_pattern = re.compile(u'('
                                   u'\ud83c[\udf00-\udfff]|'
                                   u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
                                   u'[\u2600-\u26FF\u2700-\u27BF])+',
                                   flags=re.UNICODE)
    
    text = emoji_pattern.sub(r'', text)
    
    # Replace multiple whitespace characters with a single space and strip
    return re.sub(r'\s+', ' ', text).strip()

def normalize_lang(code):
    return code.split("-")[0]

def ensure_package(from_code, to_code="en"):
    from_code = normalize_lang(from_code)
    to_code = normalize_lang(to_code)

    installed = argostranslate.translate.get_installed_languages()
    for lang in installed:
        if lang.code == from_code and any(t.to_lang.code == to_code for t in lang.translations_to):
            return

    argostranslate.package.update_package_index()
    available = argostranslate.package.get_available_packages()
    match = next((p for p in available if p.from_code == from_code and p.to_code == to_code), None)

    if match:
        argostranslate.package.install_from_path(match.download())
    else:
        raise Exception(f"No translation package for {from_code} → {to_code}")

def smart_translate(text):
    start = time.time()
    raw_code = detect(text)
    detect_duration = time.time() - start
    print(f"Detected language: {raw_code} (in {detect_duration:.4f} seconds)")

    from_code = normalize_lang(raw_code)
    ensure_package(from_code)
    return argostranslate.translate.translate(text, from_code, "en")

def main():
    data_manager = InstagramDataManager()
    reels_to_process = data_manager.get_selected_reels_with_captions()

    for pk, caption, caption_english in reels_to_process:
        # Check if already translated with meaningful content
        if caption_english and caption_english.strip():
            continue

        # Clean the original caption before any processing
        cleaned_caption = clean_text(caption)

        if not cleaned_caption:
            print(f"Reel {pk}: caption is empty or became empty after cleaning, skipping.")
            # Save an empty string to mark as processed and prevent re-processing
            data_manager.set_caption_english(pk, "")
            continue

        try:
            lang = detect(cleaned_caption)
        except Exception:
            lang = 'unknown'

        if lang == 'en' or lang == 'unknown':
            data_manager.set_caption_english(pk, cleaned_caption)
            print(f"Reel {pk}: caption is English or undetectable, copied to caption_english.")
        else:
            try:
                print(f"Reel {pk}: translating from '{lang}'...")
                translation = smart_translate(cleaned_caption)
                data_manager.set_caption_english(pk, translation)
                print(f"Reel {pk}: successfully translated to English.")
            except Exception as e:
                print(f"Reel {pk}: translation failed: {e}")
                # Save the cleaned, non-English caption to avoid re-processing a failed translation
                data_manager.set_caption_english(pk, cleaned_caption)

if __name__ == "__main__":
    main()
