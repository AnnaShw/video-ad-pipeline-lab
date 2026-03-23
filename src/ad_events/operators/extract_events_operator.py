import json
import random
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import requests

from airflow.models import BaseOperator


class ExtractEventsOperator(BaseOperator):
    """
    Custom Operator to extract raw ad events from YouTube public pages.
    """
    def __init__(self, video_ids: List[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Predefined popular YouTube videos to act as mock ad campaigns
        self.video_ids = video_ids or ["Ks-_Mh1QhMc", "uHGShqcAHlQ", "lG7DGMqcxg8", "sY2djnJVlcE", "t6IroGq2Cnk"]

    def execute(self, context) -> None:
        countries = ["US", "IL", "GB", "CA", "FR", "DE"]
        events: List[Dict] = []
        now = datetime.now(timezone.utc)
        
        for video_id in self.video_ids:
            try:
                url = f"https://www.youtube.com/watch?v={video_id}"
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                response = requests.get(url, headers=headers, timeout=10)
                html = response.text
                
                # Scrape view count using regex natively from the HTML
                view_match = re.search(r'"viewCount":"(\d+)"', html)
                view_count = int(view_match.group(1)) if view_match else random.randint(1000000, 5000000)
                
                # Simulate likes based on a traditional engagement rate (approx 2% of views)
                like_count = int(view_count * 0.02)
                
                # Scale down massive global metrics to a manageable local event stream
                num_impressions = min(max(1, view_count // 1000000), 50)
                num_clicks = min(max(1, like_count // 100000), 20)
                
                for _ in range(num_impressions):
                    events.append({
                        "ts": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
                        "event": "impression",
                        "campaign_id": video_id,
                        "country": random.choice(countries)
                    })
                    
                for _ in range(num_clicks):
                    events.append({
                        "ts": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
                        "event": "click",
                        "campaign_id": video_id,
                        "country": random.choice(countries)
                    })
            except Exception as e:
                self.log.error(f"Failed to fetch data for video {video_id}: {e}")

        # Shuffle events to simulate a realistic stream
        random.shuffle(events)
        context["ti"].xcom_push(key="raw_events", value=json.dumps(events))
