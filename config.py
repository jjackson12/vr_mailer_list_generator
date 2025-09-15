BUCKET_NAME = "vr_mail_lists"
REVIEWER_EMAIL = "jjjackson116@gmail.com"
BUCKETS_SERVICE_ACCOUNT_KEY = "vr-mail-generator-56bee8a8278b.json"
BIGQUERY_SERVICE_ACCOUNT_KEY = "vr-mail-generator-8e97a63564fe.json"
with open("mailsend_access_token.txt", "r") as file:
    MAILSEND_ACCESS_TOKEN = file.read().strip()
GMAIL_ACCESS_TOKEN = "gmail_app_credentials.json"
