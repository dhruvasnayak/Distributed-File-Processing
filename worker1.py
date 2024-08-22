import pika

def worker_callback(ch, method, properties, body):
    worker_name = method.routing_key
    print(f" [x] {worker_name} received: {body.decode()}")  # Decoding the body to convert bytes to string
    
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message

# Setup for a specific worker
worker_name = 'worker_1'  # Change this for each worker
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare and bind the queue to the exchange
channel.queue_declare(queue=f'{worker_name}_queue')
channel.queue_bind(exchange='task_exchange', queue=f'{worker_name}_queue', routing_key=worker_name)

# Consume messages
channel.basic_consume(queue=f'{worker_name}_queue', on_message_callback=worker_callback)

print(f" [*] {worker_name} waiting for tasks.")
channel.start_consuming()
