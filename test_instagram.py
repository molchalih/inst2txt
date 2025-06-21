import email
import imaplib
import json
import logging
import os.path
import re
import random
import time

import httpx

from instagrapi import Client
from instagrapi.exceptions import LoginRequired, ClientError
from instagrapi.mixins.challenge import ChallengeChoice
from pathlib import Path



# bot = {
#     "instagram_login": "ms.mark.halls728424",
#     "instagram_password": "nwJH943987",
#     "email_login": "alexafrederickson1982@aceomail.com",
#     "email_password": "noClNsjVpw",
#     "imap_server": "imap.firstmail.ltd",
#     "imap_port": "993",
#     "socks_proxy": "socks5://127.0.0.1:1080"
# }

# bot = {
#     "instagram_login": "hpgxvrwhbc",
#     "instagram_password": "6296620MxLPZY",
#     "email_login": "hpgxvrwhbc@rambler.ru",
#     "email_password": "DCfGPoY1De_D",
#     "imap_server": "imap.firstmail.ltd",
#     "imap_port": "993",
#     "socks_proxy": "socks5://127.0.0.1:1080"
# }

class Bot:
    def __init__(self):
        pass

bot = {
    "instagram_login": "martinsyvettedu94",
    "instagram_password": "T7UcjOctuxR1",
    "email_login": "jfavihgm@demainmail.com",
    "email_password": "3Dh2QVa60s",
    "imap_server": "imap.firstmail.ltd",
    "imap_port": "993",
    "socks_proxy": "socks5://127.0.0.1:1080"
}

bright_data = "7652b30c-713d-48f2-8ec6-97174f783f44"
bd = "88034b50e2a2e03515e98984a5823143534acf88f7e5dea428cc216b7ad21c1d"

CHALLENGE_EMAIL = bot["email_login"]
CHALLENGE_PASSWORD = bot["email_password"]

IG_USERNAME = bot["instagram_login"]
IG_PASSWORD = bot["instagram_password"]

PROXY = bot["socks_proxy"]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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


def get_code_from_email(username):
    mail = imaplib.IMAP4_SSL(bot["imap_server"])
    mail.login(CHALLENGE_EMAIL, CHALLENGE_PASSWORD)
    mail.select("inbox")
    result, data = mail.search(None, "(UNSEEN)")
    assert result == "OK", "Error1 during get_code_from_email: %s" % result
    ids = data.pop().split()
    for num in reversed(ids):
        mail.store(num, "+FLAGS", "\\Seen")  # mark as read
        result, data = mail.fetch(num, "(RFC822)")
        assert result == "OK", "Error2 during get_code_from_email: %s" % result
        msg = email.message_from_string(data[0][1].decode())
        payloads = msg.get_payload()
        if not isinstance(payloads, list):
            payloads = [msg]
        code = None
        for payload in payloads:
            body = payload.get_payload(decode=True).decode()
            if "<div" not in body:
                continue
            match = re.search(">([^>]*?({u})[^<]*?)<".format(u=username), body)
            if not match:
                continue
            print("Match from email:", match.group(1))
            match = re.search(r">(\d{6})<", body)
            if not match:
                print('Skip this email, "code" not found')
                continue
            code = match.group(1)
            if code:
                return code
    return False


def get_code_from_sms(username):
    while True:
        code = input(f"Enter code (6 digits) for {username}: ").strip()
        if code and code.isdigit():
            return code
    return None


def challenge_code_handler(username, choice):
    logger.info(f"Challenge triggered for {username}. {choice} is required.")
    if choice == ChallengeChoice.SMS:
        return get_code_from_sms(username)
    elif choice == ChallengeChoice.EMAIL:
        return get_code_from_email(username)
    return False


def change_password_handler(username):
    # Simple way to generate a random string
    chars = list("abcdefghijklmnopqrstuvwxyz1234567890!&£@#")
    password = "".join(random.sample(chars, 10))
    logger.info(f"Generated a new password for {username}.")
    with open("password.txt", "rw") as f:
        f.write(password)
        f.close()
    return password


def login_user():
    """
    Attempts to login to Instagram using either the provided session information
    or the provided username and password.
    """

    session_file = "session.json"

    cl = Client()

    cl.challenge_code_handler = challenge_code_handler
    cl.change_password_handler = change_password_handler
    cl.set_proxy(PROXY)

    proxy_info = get_proxy_info(PROXY)
    cl.set_country(proxy_info["country_code"])
    cl.set_locale(proxy_info["locale"])
    cl.set_timezone_offset(proxy_info["utc_offset_seconds"])

    cl.delay_range = [1, 3]

    #session = cl.load_settings(Path("session.json"))

    login_via_session = False
    login_via_pw = False

    session = None
    if os.path.exists(session_file):
        logger.info("Using existing session file to login...")
        session = cl.load_settings(Path("session.json"))

    if session:
        try:
            cl.set_settings(session)
            cl.login(IG_USERNAME, IG_PASSWORD)

            # check if session is valid
            try:
                cl.get_timeline_feed()
            except LoginRequired:
                logger.info("Session is invalid, need to login via username and password")

                old_session = cl.get_settings()

                # use the same device uuids across logins
                cl.set_settings({})
                cl.set_uuids(old_session["uuids"])

                cl.login(IG_USERNAME, IG_PASSWORD)
                cl.dump_settings(Path("session.json"))
            login_via_session = True
            return cl
        except Exception as e:
            logger.info("Couldn't login user using session information: %s" % e)

    if not login_via_session:
        try:
            logger.info("Attempting to login via username and password. username: %s" % IG_USERNAME)
            if cl.login(IG_USERNAME, IG_PASSWORD):
                login_via_pw = True
                cl.dump_settings(Path("session.json"))
                return cl
        except Exception as e:
            logger.info("Couldn't login user using username and password: %s" % e)

    if not login_via_pw and not login_via_session:
        raise Exception("Couldn't login user with either password or session")

    # if login_via_pw:
    #     cl.dump_settings(Path("session.json"))
    #
    # if login_via_session or login_via_pw:
    #     return cl


def get_followers(cl: Client, pk):
    return cl.user_followers(pk)


def get_following(cl, pk):
    return cl.user_following(pk)


if __name__ == "__main__":

    cl = login_user()

    #pk = "1730073780"
    #user_info = cl.user_info(pk)

    #pk = cl.user_id_from_username("_alina_redko_")

    user_info = cl.user_info_by_username('_alina_redko_')
    pk = user_info.pk
    following = get_following(cl, pk)
    followers = get_followers(cl, pk)

    output_data = {
        "user_info": user_info.model_dump(mode="json"),
        "following": [user.model_dump(mode="json") for user in following.values()]
    }

    # Dump to file
    with open("following.json", "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)