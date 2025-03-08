import requests
from dagster import resource
import json

class DiscordNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        
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

@resource(config_schema={"webhook_url": str})
def discord_notifier(context):
    return DiscordNotifier(
        webhook_url=context.resource_config["webhook_url"]
    )