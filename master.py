import pika
import threading
import os
import math
import json

# Expected number of replies
expected_replies = 2
reply_count = 0
received_word_counts = {}  # To store received word counts

def split_file(file_path):
    try:
        with open(file_path, 'r') as file:
            file_content = file.read()

        total_length = len(file_content)
        chunk_size = math.ceil(total_length / 2)

        chunks = [file_content[i:i + chunk_size] for i in range(0, total_length, chunk_size)]
        base_name = os.path.basename(file_path).split('.')[0]
        
        return {f"{base_name}_{i+1}": chunk for i, chunk in enumerate(chunks)}

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def send_tasks(worker_routing_keys, file_chunks):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    for i, (key, chunk) in enumerate(file_chunks.items()):
        routing_key = worker_routing_keys[i % len(worker_routing_keys)]
        message = json.dumps({'chunk_name': key, 'content': chunk})
        channel.basic_publish(exchange='task_exchange',
                              routing_key=routing_key,
                              body=message)
        print(f" [x] Sent '{message}' to {routing_key}")
    connection.close()

def master_callback(ch, method, properties, body):
    global reply_count
    global received_word_counts

    reply_count += 1
    response = json.loads(body)
    chunk_name = response['chunk_name']
    word_count = response['word_count']
    received_word_counts[chunk_name] = word_count

    print(f" [x] Master received {chunk_name} with word count {word_count} from {method.routing_key}")

    if reply_count >= expected_replies:
        ch.stop_consuming()

def consume_replies():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_consume(queue='master_reply_queue', on_message_callback=master_callback, auto_ack=True)
    print(" [*] Master waiting for replies.")
    channel.start_consuming()
    connection.close()

def sum_word_counts():
    return sum(received_word_counts.values())

# Setup RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='task_exchange', exchange_type='direct')
channel.queue_declare(queue='master_reply_queue')
channel.queue_bind(exchange='task_exchange', queue='master_reply_queue', routing_key='master_reply')
connection.close()

# List of worker routing keys
worker_routing_keys = ['worker_1', 'worker_2']

# Read input file and get output type
input_file = 'input.txt'  # Change to your input file

# Split the file and send tasks
file_chunks = split_file(input_file)
send_tasks(worker_routing_keys, file_chunks)

# Start a thread to consume replies
consuming_thread = threading.Thread(target=consume_replies)
consuming_thread.start()

# Wait for replies to be consumed
consuming_thread.join()

# Sum the word counts and print the total
total_word_count = sum_word_counts()
print(f"Total word count from all chunks: {total_word_count}")
