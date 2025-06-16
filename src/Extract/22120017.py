import json
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
from kafka.errors import KafkaTimeoutError

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def round_event_time(now:datetime, precision_ms=100):
    timestamp_ms = int(now.timestamp() * 1000)
    rounded_ms = (timestamp_ms // precision_ms) * precision_ms
    rounded_dt = datetime.fromtimestamp(rounded_ms / 1000, tz=timezone.utc)
    return rounded_dt.isoformat(timespec='milliseconds').replace("+00:00", "Z")

def fetch_and_send(event_time):
    try:
        response = requests.get('https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT', timeout=2)
        if response.status_code == 200:
            data = response.json()
            if "symbol" in data and "price" in data:
                data["event_time"] = event_time
                data["price"]= float(data["price"])
                producer.send("btc-price", value=data)
                print("Sent:", data)
    except Exception as e:
        print(f"Lỗi: {e}")


def main():
    interval = 0.1  # 100ms
    max_workers = 10

    executor = ThreadPoolExecutor(max_workers=max_workers)

    # Ghi nhận thời điểm bắt đầu
    target_time = time.time()
    
    try:
        while True:
            now = datetime.now(timezone.utc)
            event_time = round_event_time(now)
            executor.submit(fetch_and_send, event_time)

            # Tính thời gian tiếp theo cần chờ
            target_time += interval
            sleep_time = target_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("Stopping the producer")

    finally:
        print("Stopping...")
        try:
            producer.flush(timeout=3)
            producer.close(timeout=3)
            print("Completed.")
        except KafkaTimeoutError:
            print("Timeout.")

if __name__ == "__main__":
    main()

# python3 src/Extract/22120017.py