from langdetect import detect
import argostranslate.package, argostranslate.translate
import time
from db_manager import InstagramDataManager

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

def process_captions():
    data_manager = InstagramDataManager()
    reels = data_manager.get_selected_reels_with_captions()
    for pk, caption in reels:
        if not caption:
            print(f"Reel {pk}: empty caption, skipping.")
            continue
        # Check if already translated
        import sqlite3
        conn = sqlite3.connect('data/instagram_data.db')
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(reels)")
        columns = [row[1] for row in cursor.fetchall()]
        already_translated = False
        if 'caption_english' in columns:
            cursor.execute("SELECT caption_english FROM reels WHERE pk = ?", (pk,))
            row = cursor.fetchone()
            if row and row[0]:
                already_translated = True
        conn.close()
        if already_translated:
            print(f"Reel {pk}: already has caption_english, skipping.")
            continue
        try:
            lang = detect(caption)
        except Exception:
            lang = 'unknown'
        if lang == 'en' or lang == 'unknown':
            data_manager.set_caption_english(pk, caption)
            print(f"Reel {pk}: caption is English or undetectable, copied.")
        else:
            try:
                translation = smart_translate(caption)
                data_manager.set_caption_english(pk, translation)
                print(f"Reel {pk}: translated to English.")
            except Exception as e:
                print(f"Reel {pk}: translation failed: {e}")
                data_manager.set_caption_english(pk, caption)

if __name__ == "__main__":
    process_captions()
