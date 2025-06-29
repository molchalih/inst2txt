import httpx

def get_proxy_info(proxy_url: str, retries: int = 2, delay: int = 3):

    try:
        with httpx.Client(proxy=proxy_url, timeout=10) as client:
            response = client.get("https://ipapi.co/json/")
            data = response.json()

        # Extract and parse language, country, timezone
        language = data.get("languages", "nl").split(",")[0].split("-")[0]
        country_code = data.get("country", "NL")
        locale = f"{language}_{country_code}"

        raw_offset = data.get("utc_offset", "+0200")
        sign = 1 if raw_offset.startswith("+") else -1
        hours = int(raw_offset[1:3])
        minutes = int(raw_offset[3:5])
        offset_seconds = sign * (hours * 3600 + minutes * 60)

        return {
            "country_code": country_code,
            "locale": locale,
            "utc_offset_seconds": offset_seconds
        }

    except Exception as e:
        print(f"❌ Returning default values. Failed to get proxy location info: {e}")

        return {
            "country_code": "NL",
            "locale": "nl_NL",
            "utc_offset_seconds": "+0200"
        }