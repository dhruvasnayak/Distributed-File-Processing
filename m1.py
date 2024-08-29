import os
import pika
import threading
import json
import pandas as pd
from sklearn.ensemble import VotingClassifier
from joblib import dump, load
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score

expected_replies = 3
reply_count = 0
received_models = []
model_accuracies = []

def send_tasks(worker_routing_keys, data):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Convert the entire dataset to JSON
    data_json = data.to_json(orient='split')
    message = json.dumps({'data': data_json})
    
    for routing_key in worker_routing_keys:
        channel.basic_publish(exchange='model_training_exchange',
                              routing_key=routing_key,
                              body=message)
        print(f" [x] Sent entire dataset to {routing_key}")
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
worker_routing_keys = ['worker_1', 'worker_2', 'worker_3']

# Load the dataset
data = pd.read_csv("framingham.csv")
data = data[['sysBP', 'glucose', 'age', 'cigsPerDay', 'totChol', 'diaBP', 'prevalentHyp', 'male', 'BPMeds', 'diabetes', 'TenYearCHD']]

# Handle missing values
imputer = SimpleImputer(strategy='mean')
data_imputed = pd.DataFrame(imputer.fit_transform(data), columns=data.columns)

# Send the entire dataset to workers
send_tasks(worker_routing_keys, data_imputed)

# Start a thread to consume replies
consuming_thread = threading.Thread(target=consume_replies)
consuming_thread.start()

# Wait for replies to be consumed
consuming_thread.join()

# Combine the models
ensemble_model = combine_models()
print(f"Ensemble model created with {len(received_models)} models.")

# Fit the ensemble model on the same dataset used for training
X_train = data_imputed.iloc[:, :-1]  # Features
y_train = data_imputed.iloc[:, -1]   # Labels
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
