import pika

def worker_callback(ch, method, properties, body):
    worker_name = method.routing_key
    print(f" [x] {worker_name} received {body}")
    
    # Simulate processing and send a reply
    reply_message = f"Reply from {worker_name}"
    ch.basic_publish(exchange='task_exchange',
                     routing_key='master_reply',
                     body=reply_message)
    print(f" [x] {worker_name} sent {reply_message}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Setup for a specific worker
worker_name = 'worker_2'  # This will differ for each worker
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f'{worker_name}_queue')
channel.queue_bind(exchange='task_exchange', queue=f'{worker_name}_queue', routing_key=worker_name)

channel.basic_consume(queue=f'{worker_name}_queue', on_message_callback=worker_callback)

print(f" [*] {worker_name} waiting for tasks.")
channel.start_consuming()
