import pika

def setup_rabbitmq():
    """Set up RabbitMQ exchanges and queues."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the exchange if it doesn't already exist
    try:
        channel.exchange_declare(exchange='model_training_exchange', exchange_type='direct', durable=True)
    except pika.exceptions.ChannelClosedByBroker as e:
        print(f"Exchange already exists or other error: {e}")

    # Declare the master reply queue if it doesn't already exist
    try:
        channel.queue_declare(queue='master_reply_queue', durable=True)
    except pika.exceptions.ChannelClosedByBroker as e:
        print(f"Queue already exists or other error: {e}")

    # Bind the queue to the exchange
    try:
        channel.queue_bind(exchange='model_training_exchange', queue='master_reply_queue', routing_key='master_reply')
    except pika.exceptions.ChannelClosedByBroker as e:
        print(f"Queue binding already exists or other error: {e}")

    connection.close()

def main():
    """Main function to run the RabbitMQ setup."""
    setup_rabbitmq()
    print("RabbitMQ setup completed.")

if __name__ == "__main__":
    main()
