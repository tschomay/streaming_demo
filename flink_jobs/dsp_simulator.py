import json
import time
import random
import uuid
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  # Internal Docker hostname
TOPIC_REQUESTS = 'dsp_requests'
TOPIC_WINS = 'dsp_wins'
TOPIC_CLICKS = 'dsp_clicks'

# --- SIMULATION PARAMETERS ---
NUM_USERS = 100
PUBLISHERS = [
    {"id": "pub_001", "domain": "techcrunch.com", "category": "Tech"},
    {"id": "pub_002", "domain": "vogue.com", "category": "Lifestyle"},
    {"id": "pub_003", "domain": "espn.com", "category": "Sports"},
    {"id": "pub_004", "domain": "cnn.com", "category": "News"},
    {"id": "pub_005", "domain": "wired.com", "category": "Tech"},
]

class User:
    def __init__(self, user_id):
        self.user_id = user_id
        # Randomly assign a "persona" that influences behavior
        self.persona = random.choice(["Techie", "Fashionista", "Jock", "General"])
        self.ip_address = f"192.168.0.{random.randint(1, 255)}"
        self.device_os = random.choice(["iOS", "Android", "Windows", "MacOS"])
        
        # Define affinities (probability to click based on category)
        self.affinities = {
            "Tech": 0.01,
            "Lifestyle": 0.01,
            "Sports": 0.01,
            "News": 0.01
        }
        self._set_preferences()

    def _set_preferences(self):
        # Hidden Signal: Users behave differently based on persona
        # The ML model will ideally learn this pattern later!
        if self.persona == "Techie":
            self.affinities["Tech"] = 0.15      # High CTR on Tech
        elif self.persona == "Fashionista":
            self.affinities["Lifestyle"] = 0.12 # High CTR on Lifestyle
        elif self.persona == "Jock":
            self.affinities["Sports"] = 0.10    # High CTR on Sports

    def browse(self):
        """Simulates a browsing session. Returns Request or None."""
        if random.random() > 0.8: # Only browse 20% of the time ticks
            pub = random.choice(PUBLISHERS)
            request = {
                "request_id": str(uuid.uuid4()),
                "timestamp_ts": int(time.time() * 1000),
                "user_id": self.user_id,
                "ip_address": self.ip_address,
                "os": self.device_os,
                "site_domain": pub["domain"],
                "site_category": pub["category"], # In real life, we might not know this, but we'll include it for now
                "publisher_id": pub["id"]
            }
            return request, pub["category"]
        return None, None

def main():
    print("Initializing Kafka Producer...")
    # Retrying connection logic for startup resilience
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka!")
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
            time.sleep(5)

    # Initialize User Pool
    users = [User(f"u_{i:03d}") for i in range(NUM_USERS)]
    
    print(f"Starting simulation with {NUM_USERS} users...")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            for user in users:
                # 1. REQUEST EVENT
                request_data, category = user.browse()
                
                if request_data:
                    producer.send(TOPIC_REQUESTS, request_data)
                    
                    # 2. BID/WIN LOGIC (Simplified DSP Logic)
                    # We bid if the user matches some basic targeting, or randomly.
                    # Let's assume we win 60% of auctions we enter.
                    should_bid = True 
                    did_win = True if random.random() < 0.6 else False

                    if should_bid and did_win:
                        win_data = {
                            "request_id": request_data["request_id"],
                            "auction_id": str(uuid.uuid4()),
                            "timestamp_ts": int(time.time() * 1000) + random.randint(50, 200), # slightly later
                            "campaign_id": "camp_101",
                            "creative_id": "creat_55",
                            "bid_price": round(random.uniform(0.5, 5.0), 2),
                            "win_price": round(random.uniform(0.5, 5.0), 2)
                        }
                        producer.send(TOPIC_WINS, win_data)

                        # 3. CLICK EVENT (Probabilistic)
                        # Does the user click? Use their hidden affinity + some randomness
                        click_prob = user.affinities.get(category, 0.01)
                        
                        if random.random() < click_prob:
                            click_data = {
                                "request_id": request_data["request_id"],
                                "timestamp_ts": win_data["timestamp_ts"] + random.randint(1000, 10000), # 1-10s later
                                "user_action": "click"
                            }
                            producer.send(TOPIC_CLICKS, click_data)

            producer.flush()
            time.sleep(1.0) # Slow down the loop to make it readable

    except KeyboardInterrupt:
        print("\nSimulation stopped.")
        producer.close()

if __name__ == "__main__":
    main()
