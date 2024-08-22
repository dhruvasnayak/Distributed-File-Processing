import pika

# Connect to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare exchange and queues
channel.exchange_declare(exchange='task_exchange', exchange_type='direct')

# Sending messages to workers
worker_routing_keys = ['worker_1']

for i, routing_key in enumerate(worker_routing_keys):
    message = f"Task {i+1} for {routing_key}"
    channel.basic_publish(exchange='task_exchange',
                          routing_key=routing_key,
                          body=message)
    print(f" [x] Sent '{message}' to {routing_key}")

connection.close()
