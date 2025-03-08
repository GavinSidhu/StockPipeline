import requests
import os
from dagster import resource
import json

class DiscordNotifier:
    def __init__(self, webhook_url=None):
        # If webhook_url is not provided, try to get it from environment
        self.webhook_url = webhook_url or os.environ.get("DISCORD_WEBHOOK_URL")
        if not self.webhook_url:
            raise ValueError("Discord webhook URL not provided and DISCORD_WEBHOOK_URL environment variable not set")
        
    def send_notification(self, message, embeds=None, username="Stock ETL Bot"):
        """Send a Discord notification."""
        payload = {
            "content": message,
            "username": username
        }
        
        if embeds:
            payload["embeds"] = embeds
            
        response = requests.post(
            self.webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )
        
        response.raise_for_status()
        return response

@resource
def discord_notifier(context):
    return DiscordNotifier()