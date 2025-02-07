import json
import signal
import pika
import os
import time
import logging

from database import MongoDB, update_mongo

# Load environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.getenv("QUEUE_NAME", "task_queue")
DLQ_NAME = os.getenv("DLQ_NAME", "task_queue_dlq")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_message(ch, method, properties, body):
    """Process the message from RabbitMQ."""
    try:
        message = json.loads(body.decode())
        logging.info(f"Processing message: {message}")

        # Simulated processing
        time.sleep(2)
        if "fail" in body.decode():
            raise Exception("Simulated processing failure")

        # ✅ Update MongoDB after processing
        update_mongo(message)

        logging.info(f"Message processed successfully: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logging.error(f"Processing failed: {e}")
        ch.basic_nack(
            delivery_tag=method.delivery_tag, requeue=False
        )  # Send to DLQ


def main():
    """Consume messages from RabbitMQ."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=DLQ_NAME, durable=True)

    logging.info("Main Consumer started. Listening for messages...")
    channel.basic_consume(
        queue=QUEUE_NAME, on_message_callback=process_message, auto_ack=False
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Main Consumer stopped.")
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
