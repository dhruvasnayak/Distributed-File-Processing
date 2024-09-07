import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='model_training_exchange', exchange_type='direct')
channel.queue_declare(queue='master_reply_queue')
channel.queue_bind(exchange='model_training_exchange', queue='master_reply_queue', routing_key='master_reply')
connection.close()