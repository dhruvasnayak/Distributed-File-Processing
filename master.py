import pika
import threading

# Expected number of replies
expected_replies = 2
reply_count = 0

# Function to send tasks to workers
def send_tasks(worker_routing_keys):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    for i, routing_key in enumerate(worker_routing_keys):
        message = f"Task {i+1} for {routing_key}"
        channel.basic_publish(exchange='task_exchange',
                              routing_key=routing_key,
                              body=message)
        print(f" [x] Sent '{message}' to {routing_key}")
    connection.close()

# Function to handle replies from workers
def master_callback(ch, method, properties, body):
    global reply_count
    reply_count += 1
    print(f" [x] Master received {body} from {method.routing_key}")
    if reply_count >= expected_replies:
        ch.stop_consuming()

# Function to start consuming replies
def consume_replies():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_consume(queue='master_reply_queue', on_message_callback=master_callback, auto_ack=True)
    print(" [*] Master waiting for replies.")
    channel.start_consuming()
    connection.close()

# Establish connection to RabbitMQ and set up the exchange and reply queue
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='task_exchange', exchange_type='direct')
channel.queue_declare(queue='master_reply_queue')
channel.queue_bind(exchange='task_exchange', queue='master_reply_queue', routing_key='master_reply')
connection.close()

# List of worker routing keys
worker_routing_keys = ['worker_1', 'worker_2']

# Start a thread to send tasks
sending_thread = threading.Thread(target=send_tasks, args=(worker_routing_keys,))
sending_thread.start()

# Start a thread to consume replies
consuming_thread = threading.Thread(target=consume_replies)
consuming_thread.start()

# Wait for both threads to finish
sending_thread.join()
consuming_thread.join()
