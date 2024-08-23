import pika
import json

def count_words(content):
    return len(content.split())

def worker_callback(ch, method, properties, body):
    worker_name = method.routing_key
    message = json.loads(body)
    chunk_name = message['chunk_name']
    content = message['content']

    print(f" [x] {worker_name} received chunk {chunk_name}")

    # Count words in the chunk
    word_count = count_words(content)
    
    reply_message = json.dumps({'chunk_name': chunk_name, 'word_count': word_count})
    ch.basic_publish(exchange='file_processing_exchange',
                     routing_key='master_reply',
                     body=reply_message)
    print(f" [x] {worker_name} sent {reply_message}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Setup for a specific worker
worker_name = 'worker_2'  # Change for each worker
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f'{worker_name}_queue')
channel.queue_bind(exchange='file_processing_exchange', queue=f'{worker_name}_queue', routing_key=worker_name)

channel.basic_consume(queue=f'{worker_name}_queue', on_message_callback=worker_callback)

print(f" [*] {worker_name} waiting for tasks.")
channel.start_consuming()
