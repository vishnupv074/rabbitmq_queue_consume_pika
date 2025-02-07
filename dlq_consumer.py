import signal
import pika
import os
import time
import logging
import json
from database import MongoDB, save_to_database, update_mongo
from publisher import publish_message

# Load environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
MAIN_QUEUE = os.getenv("QUEUE_NAME", "task_queue")
DLQ_NAME = os.getenv("DLQ_NAME", "task_queue_dlq")
PROCESSED_QUEUE = os.getenv("PROCESSED_QUEUE", "status_queue")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_failed_message(ch, method, properties, body):
    """Retry processing failed messages from DLQ with exponential backoff."""
    try:
        # Decode message
        message = json.loads(body.decode())
        retry_count = properties.headers.get("x-retry-count", 0) + 1

        logging.info(
            f"Retrying message [{retry_count}/{MAX_RETRIES}]: {message}"
        )

        # Simulated processing failure
        if retry_count < MAX_RETRIES:
            raise Exception("Simulated failure")

        # ✅ Update MongoDB on successful processing
        if update_mongo(message):
            logging.info("Data successfully updated in MongoDB")

            # Send processed message to another queue
            publish_message(PROCESSED_QUEUE, message)

        logging.info(f"Successfully reprocessed: {message}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logging.error(f"Invalid JSON received: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge bad JSON

    except Exception as e:
        logging.error(f"Retry failed: {e}")

        if retry_count >= MAX_RETRIES:
            logging.error(f"Max retries reached. Storing in DB: {message}")
            message["retry_count"] = (
                retry_count  # Store retry count for reference
            )
            save_to_database(message)  # Store structured JSON in DB
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            headers = properties.headers or {}
            headers["x-retry-count"] = retry_count
            backoff_time = 2**retry_count
            logging.info(f"Waiting {backoff_time}s before retrying...")
            time.sleep(backoff_time)

            ch.basic_publish(
                exchange="",
                routing_key=MAIN_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    headers=headers, delivery_mode=2
                ),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    """Consume messages from DLQ and retry processing."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.queue_declare(queue=DLQ_NAME, durable=True)

    logging.info("DLQ Consumer started. Listening for failed messages...")
    channel.basic_consume(
        queue=DLQ_NAME,
        on_message_callback=process_failed_message,
        auto_ack=False,
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("DLQ Consumer stopped.")
        channel.stop_consuming()
        connection.close()


def graceful_exit(signum, frame):
    """Ensure MongoDB connection is closed on shutdown."""
    logging.info("Shutting down consumer...")
    MongoDB.close_client()
    exit(0)


signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)


if __name__ == "__main__":
    main()
