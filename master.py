import os
import pika
import threading
import json
import pandas as pd
from sklearn.ensemble import VotingClassifier
from joblib import dump, load
from sklearn.datasets import load_iris
from sklearn.metrics import accuracy_score

expected_replies = 2
reply_count = 0
received_models = []
model_accuracies = []

def split_dataset(data):
    """Split the dataset into chunks."""
    chunk_size = len(data) // 2
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    return chunks

def send_tasks(worker_routing_keys, data_chunks):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    for i, chunk in enumerate(data_chunks):
        routing_key = worker_routing_keys[i % len(worker_routing_keys)]
        chunk_json = chunk.to_json(orient='split')
        message = json.dumps({'chunk_id': i + 1, 'data_chunk': chunk_json})
        channel.basic_publish(exchange='model_training_exchange',
                              routing_key=routing_key,
                              body=message)
        print(f" [x] Sent data chunk to {routing_key}")
    connection.close()

def master_callback(ch, method, properties, body):
    global reply_count
    global received_models
    global model_accuracies

    reply_count += 1
    response = json.loads(body)
    model_file = response['model_file']
    accuracy = response['accuracy']

    if not os.path.isfile(model_file):
        print(f" [!] Model file {model_file} not found.")
        return

    # Load the received model
    model = load(model_file)
    received_models.append(model)
    model_accuracies.append(accuracy)

    print(f" [x] Master received model from {method.routing_key} with accuracy {accuracy}")

    if reply_count >= expected_replies:
        ch.stop_consuming()

def consume_replies():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_consume(queue='master_reply_queue', on_message_callback=master_callback, auto_ack=True)
    print(" [*] Master waiting for model replies.")
    channel.start_consuming()
    connection.close()

def combine_models():
    """Combine all received models into a VotingClassifier."""
    voting_clf = VotingClassifier(estimators=[(f'model_{i}', model) for i, model in enumerate(received_models)],
                                  voting='soft')
    return voting_clf

def evaluate_ensemble_model(ensemble_model, X_test, y_test):
    """Evaluate the ensemble model."""
    y_pred = ensemble_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    return accuracy

# Setup RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the exchange
channel.exchange_declare(exchange='model_training_exchange', exchange_type='direct')

# Declare the reply queue
channel.queue_declare(queue='master_reply_queue')
channel.queue_bind(exchange='model_training_exchange', queue='master_reply_queue', routing_key='master_reply')

connection.close()

# List of worker routing keys
worker_routing_keys = ['worker_1', 'worker_2']

# Load Iris dataset and split it
iris = load_iris()
df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
df['target'] = iris.target
data_chunks = split_dataset(df)

# Send data chunks to workers
send_tasks(worker_routing_keys, data_chunks)

# Start a thread to consume replies
consuming_thread = threading.Thread(target=consume_replies)
consuming_thread.start()

# Wait for replies to be consumed
consuming_thread.join()

# Combine the models
ensemble_model = combine_models()
print(f"Ensemble model created with {len(received_models)} models.")

# Fit the ensemble model on the same dataset used for training
X_train = df.iloc[:, :-1]  # Features
y_train = df.iloc[:, -1]   # Labels
ensemble_model.fit(X_train, y_train)
print("Ensemble model fitted.")

# Save the ensemble model
dump(ensemble_model, 'ensemble_model.pkl')
print("Ensemble model saved as 'ensemble_model.pkl'.")

# Evaluate each model and the ensemble model
for i, accuracy in enumerate(model_accuracies):
    print(f"Model {i+1} accuracy: {accuracy}")

# Evaluate ensemble model
ensemble_accuracy = evaluate_ensemble_model(ensemble_model, X_train, y_train)
print(f"Ensemble model accuracy: {ensemble_accuracy}")
