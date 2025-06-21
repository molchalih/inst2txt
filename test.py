"""
Example to handle Email/SMS challenges
"""
import email
import email.message
import imaplib
import logging
import re
import random

from instagrapi import Client
from instagrapi.mixins.challenge import ChallengeChoice


bot = {
    "instagram_login": "martinsyvettedu94",
    "instagram_password": "T7UcjOctuxR1",
    "email_login": "jfavihgm@demainmail.com",
    "email_password": "3Dh2QVa60s",
    "imap_server": "imap.firstmail.ltd",
    "imap_port": "993",
    "socks_proxy": "socks5://127.0.0.1:1080"
}

CHALLENGE_EMAIL = bot["email_login"]
CHALLENGE_PASSWORD = bot["email_password"]

IG_USERNAME = bot["instagram_login"]
IG_PASSWORD = bot["instagram_password"]

PROXY = bot["socks_proxy"]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_code_from_email(username):
    print(f"🔍 get_code_from_email called for username: {username}")
    print(f"📧 Connecting to email server: {bot['imap_server']}:{bot['imap_port']}")
    
    try:
        mail = imaplib.IMAP4_SSL(bot["imap_server"], int(bot["imap_port"]))
        print("✅ Connected to email server successfully")
        
        mail.login(CHALLENGE_EMAIL, CHALLENGE_PASSWORD)
        print(f"✅ Logged into email: {CHALLENGE_EMAIL}")
        
        mail.select("inbox")
        print("✅ Selected inbox")
        
        result, data = mail.search(None, "(UNSEEN)")
        assert result == "OK", "Error1 during get_code_from_email: %s" % result
        print(f"📬 Found {len(data[0].split()) if data[0] else 0} unread emails")
        
        ids = data.pop().split()
        for num in reversed(ids):
            print(f"📧 Processing email #{num.decode()}")
            mail.store(num, "+FLAGS", "\\Seen")  # mark as read
            result, data = mail.fetch(num, "(RFC822)")
            assert result == "OK", "Error2 during get_code_from_email: %s" % result
            if not data or not data[0]:
                print(f"   ❌ No data received for email #{num.decode()}")
                continue
            email_data = data[0][1]
            if isinstance(email_data, bytes):
                msg = email.message_from_string(email_data.decode())
            else:
                msg = email.message_from_string(str(email_data))
            payloads = msg.get_payload()
            if not isinstance(payloads, list):
                payloads = [msg]
            code = None
            for payload in payloads:
                if isinstance(payload, email.message.Message):
                    payload_data = payload.get_payload(decode=True)
                    body = payload_data.decode() if isinstance(payload_data, bytes) else str(payload_data)
                else:
                    body = str(payload)
                if "<div" not in body:
                    print("   ⏭️  Skipping email - no HTML content")
                    continue
                match = re.search(">([^>]*?({u})[^<]*?)<".format(u=username), body)
                if not match:
                    print(f"   ⏭️  Skipping email - username '{username}' not found")
                    continue
                print("Match from email:", match.group(1))
                match = re.search(r">(\d{6})<", body)
                if not match:
                    print('   ❌ Skip this email, "code" not found')
                    continue
                code = match.group(1)
                if code:
                    print(f"   ✅ Found code: {code}")
                    return code
        print("❌ No valid code found in any emails")
        return ""
        
    except Exception as e:
        print(f"❌ Error in get_code_from_email: {e}")
        return ""


def get_code_from_sms(username):
    print(f"📱 get_code_from_sms called for username: {username}")
    while True:
        code = input(f"Enter code (6 digits) for {username}: ").strip()
        if code and code.isdigit():
            print(f"✅ SMS code entered: {code}")
            return code
    return ""


def challenge_code_handler(username: str, choice=None):
    print(f"🔐 challenge_code_handler called for username: {username}, choice: {choice}")
    if choice == ChallengeChoice.SMS:
        print("📱 Using SMS challenge method")
        return get_code_from_sms(username)
    elif choice == ChallengeChoice.EMAIL:
        print("📧 Using EMAIL challenge method")
        return get_code_from_email(username)
    print(f"❌ Unknown challenge choice: {choice}")
    return ""


def change_password_handler(username):
    # Simple way to generate a random string
    chars = list("abcdefghijklmnopqrstuvwxyz1234567890!&£@#")
    password = "".join(random.sample(chars, 10))
    return password


if __name__ == "__main__":
    cl = Client()
    cl.challenge_code_handler = challenge_code_handler
    cl.change_password_handler = change_password_handler
    cl.set_proxy(PROXY)
    
    try:
        print(f"Attempting to login with username: {IG_USERNAME}")
        cl.login(IG_USERNAME, IG_PASSWORD)
        print("Login successful!")
    except Exception as e:
        print(f"Login failed: {e}")
        print("You may need to manually complete the challenge on Instagram.com first")