from webhook.webhook import send_webhook_message
import const.const as const
import sys

print(sys.path)

send_webhook_message('hello')
print(const.PI)
