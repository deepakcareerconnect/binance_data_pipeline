
import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError
from websocket import WebSocketApp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC", "binance.trades.raw"
)

STREAMS = [
    "btcusdt@trade",
    "ethusdt@trade",
    "solusdt@trade",
]

WEBSOCKET_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(STREAMS)
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, separators=(",", ":")
        ).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=30000,
    )


def main():
    producer = create_producer()

    def on_message(ws, message):
        try:
            envelope = json.loads(message)
            event = envelope.get("data", envelope)

            required = ["e", "s", "t", "p", "q", "T"]
            if not all(field in event for field in required):
                logger.warning("Skipping malformed trade event")
                return

            if event["e"] != "trade":
                logger.warning("Skipping unexpected event type")
                return

            symbol = event["s"]

            record = {
                "source": "binance_spot_websocket",
                "event_type": event["e"],
                "symbol": symbol,
                "trade_id": int(event["t"]),
                "price": str(event["p"]),
                "quantity": str(event["q"]),
                "trade_time_ms": int(event["T"]),
                "event_time_ms": int(event["E"]),
                "is_buyer_market_maker": bool(event["m"]),
                "ingested_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "raw_event": event,
            }

            future = producer.send(
                KAFKA_TOPIC,
                key=symbol,
                value=record,
            )

            # Wait for broker acknowledgement before logging success.
            metadata = future.get(timeout=10)

            logger.info(
                "Kafka publish OK | symbol=%s | trade_id=%s "
                "| partition=%s | offset=%s",
                symbol,
                record["trade_id"],
                metadata.partition,
                metadata.offset,
            )

        except (ValueError, KeyError, TypeError, KafkaError) as exc:
            logger.exception("Trade event processing failed: %s", exc)

    def on_error(ws, error):
        logger.error("WebSocket error: %s", error)

    def on_close(ws, status_code, message):
        logger.warning(
            "WebSocket closed | status=%s | message=%s",
            status_code,
            message,
        )

    def on_open(ws):
        logger.info("Connected to Binance Spot WebSocket")

    try:
        while True:
            ws = WebSocketApp(
                WEBSOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            # Returns after a disconnect; outer loop reconnects.
            ws.run_forever(ping_interval=20, ping_timeout=10)

            logger.warning("Disconnected; reconnecting in 5 seconds")
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Stopping WebSocket producer")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
