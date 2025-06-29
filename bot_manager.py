import json
import logging
import random
from pathlib import Path
from typing import Optional

from instagrapi import Client
from instagrapi.exceptions import LoginRequired
from instagrapi.mixins.challenge import ChallengeChoice

from helpers.proxy import get_proxy_info
from helpers.verification import get_code_from_email, get_code_from_sms

logger = logging.getLogger(__name__)

class Bot:
    def __init__(self, bot_credentials: dict):
        self.instagram_login = bot_credentials["instagram_login"]
        self.instagram_password = bot_credentials["instagram_password"]
        self.email_login = bot_credentials["email_login"]
        self.email_password = bot_credentials["email_password"]
        self.imap_server = bot_credentials["imap_server"]
        self.imap_port = bot_credentials["imap_port"]
        self.socks_proxy = bot_credentials["socks_proxy"]
        self.client = self.login_user()

    def challenge_code_handler(self, username: str, choice=None):
        logger.info(f"Challenge triggered for {username}. {choice} is required.")
        if choice == ChallengeChoice.SMS:
            return get_code_from_sms(username)
        elif choice == ChallengeChoice.EMAIL:
            return get_code_from_email(username, self.email_login, self.email_password, self.imap_server, self.imap_port)
        return ""

    def change_password_handler(self, username: str):
        chars = list("abcdefghijklmnopqrstuvwxyz1234567890!&£@#")
        password = "".join(random.sample(chars, 10))
        logger.info(f"Generated a new password for {username}.")

        # This assumes the bots are in a 'data' subdirectory.
        bots_json_path = Path('data/bots.json')
        with open(bots_json_path, 'r') as f:
            bots_data = json.load(f)

        for bot_item in bots_data:
            if bot_item['instagram_login'] == username:
                bot_item['instagram_password'] = password
                self.instagram_password = password
                break
        
        with open(bots_json_path, 'w') as f:
            json.dump(bots_data, f, indent=4)

        logger.info(f"Successfully updated password for {username} in bots.json")
        return password

    def login_user(self):
        """
        Attempts to login to Instagram using either the provided session information
        or the provided username and password.
        """
        cl = Client()
        cl.challenge_code_handler = self.challenge_code_handler
        cl.change_password_handler = self.change_password_handler

        proxy_info = get_proxy_info(self.socks_proxy)
        cl.set_country(proxy_info["country_code"])
        cl.set_locale(proxy_info["locale"])
        cl.set_timezone_offset(int(proxy_info["utc_offset_seconds"]))

        Path("sessions").mkdir(parents=True, exist_ok=True)
        session_file = Path(f"sessions/{self.instagram_login}.json")

        session = None

        if session_file.exists():
            if session_file.read_text() != "":
                try:
                    session = cl.load_settings(session_file)
                    logger.info(f"Loaded session for {self.instagram_login} from {session_file}")
                except Exception as e:
                    logger.warning(f"Could not load session from {session_file}: {e}")
                    session_file.write_text("") # Clear broken session file
        else:
            session_file.write_text("{}")
        
        cl.delay_range = [2, 5]

        if session:
            try:
                cl.set_settings(session)
                cl.login(self.instagram_login, self.instagram_password)
                cl.get_timeline_feed()
                logger.info(f"✅ Logged in to Instagram as {self.instagram_login} using session.")
                return cl
            except LoginRequired:
                logger.info("Session is invalid, will login via username and password.")
                old_session = cl.get_settings()
                cl.set_settings({})
                if "uuids" in old_session:
                    cl.set_uuids(old_session["uuids"])
            except Exception as e:
                logger.info(f"Couldn't login using session for {self.instagram_login}: {e}")

        try:
            logger.info(f"Attempting to login via username and password for {self.instagram_login}")
            if cl.login(self.instagram_login, self.instagram_password):
                cl.dump_settings(session_file)
                logger.info(f"✅ Logged in and saved session to {session_file}")
                return cl
        except Exception as e:
            logger.error(f"Couldn't login user {self.instagram_login} using username/password: {e}")

        logger.error(f"❌ Failed to login to Instagram as {self.instagram_login}.")
        return None

class BotManager:
    def __init__(self, bots_config_path='data/bots.json'):
        self.bots_config_path = bots_config_path
        self.bots = self._load_bots()

    def _load_bots(self):
        try:
            with open(self.bots_config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Bots config file not found at {self.bots_config_path}")
            return []
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from {self.bots_config_path}")
            return []

    def get_bot_client(self, bot_index: int) -> Optional[Client]:
        if not self.bots or bot_index >= len(self.bots):
            logger.error(f"Bot index {bot_index} is out of range.")
            return None
        
        bot_credentials = self.bots[bot_index]
        bot = Bot(bot_credentials)
        
        return bot.client 