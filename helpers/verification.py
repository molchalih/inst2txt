import email
import email.message
import imaplib
import re
import time


def get_code_from_email(username, email_login, email_password, imap_server, imap_port):    
    print(f"🔍 get_code_from_email called for username: {username}")
    print("⏳ Waiting 3 seconds for email to arrive...")
    time.sleep(3)  # Wait for email to arrive
    print(f"📧 Connecting to email server: {imap_server}:{imap_port}")
    
    try:
        mail = imaplib.IMAP4_SSL(imap_server, int(imap_port))
        
        mail.login(email_login, email_password)
        print(f"✅ Logged into email: {email_login}")
        
        mail.select("inbox")
        
        result, data = mail.search(None, "(UNSEEN)")
        assert result == "OK", "Error1 during get_code_from_email: %s" % result
        
        ids = data.pop().split()
        if not ids:
            print("❌ No unseen emails found")
            return ""
        
        # Get only the last (most recent) unseen email
        last_email_id = ids[-1]  # Get the last email ID (most recent)
        
        mail.store(last_email_id, "+FLAGS", "\\Seen")  # mark as read
        result, data = mail.fetch(last_email_id, "(RFC822)")
        assert result == "OK", "Error2 during get_code_from_email: %s" % result
        if not data or not data[0]:
            print(f"   ❌ No data received for email #{last_email_id.decode()}")
            return ""
        
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
        
        print("❌ No valid code found in the last unseen email")
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