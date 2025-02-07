import os
import json
import logging
import pika

# Load environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def publish_message(queue, message):
    """Publish a JSON message to RabbitMQ."""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
        channel = connection.channel()

        # Declare queue (make sure it's durable)
        channel.queue_declare(queue=queue, durable=True)

        # Publish message
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2
            ),  # Persistent messages
        )

        logging.info(f"Published message to '{queue}': {message}")

        # Close connection
        connection.close()
    except Exception as e:
        logging.error(f"Failed to publish message: {e}")
