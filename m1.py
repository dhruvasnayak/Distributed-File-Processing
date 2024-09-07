import os
import pika
import threading
import json
import pandas as pd
from joblib import dump, load
import base64
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import VotingClassifier
from sklearn.metrics import accuracy_score
from sklearn.impute import SimpleImputer
from io import BytesIO

expected_replies = 2
reply_count = 0
received_models = []
model_accuracies = []

def deserialize_model(model_serialized):
    """Deserialize a base64 encoded model."""
    model_data = base64.b64decode(model_serialized)
    model = load(BytesIO(model_data))
    return model

def serialize_model(model):
    """Serialize and base64 encode the model."""
    buffer = BytesIO()
    dump(model, buffer)
    buffer.seek(0)
    model_serialized = base64.b64encode(buffer.read()).decode('utf-8')
    return model_serialized

def send_task_to_worker(worker_routing_key, data, model):
    """Send data and a serialized model to a worker."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Convert the entire dataset to JSON
    data_json = data.to_json(orient='split')

    # Serialize and encode the model
    model_serialized = serialize_model(model)
    
    # Determine model type
    model_type = 'logistic_regression' if isinstance(model, LogisticRegression) else 'knn'
    
    # Create the message
    message = json.dumps({
        'data': data_json,
        'model': model_serialized,
        'model_type': model_type
    })
    
    # Send the message to the worker
    channel.basic_publish(exchange='model_training_exchange',
                          routing_key=worker_routing_key,
                          body=message)
    print(f" [x] Sent data and model to {worker_routing_key}")
    connection.close()

def master_callback(ch, method, properties, body):
    """Handle replies from workers."""
    global reply_count
    global received_models
    global model_accuracies

    reply_count += 1
    response = json.loads(body)
    
    model_serialized = response.get('model')
    accuracy = response.get('accuracy')
    
    if model_serialized is None or accuracy is None:
        print(f" [x] Incomplete response received: {response}")
        return
    
    # Deserialize the received model
    model = deserialize_model(model_serialized)
    
    received_models.append(model)
    model_accuracies.append(accuracy)
    
    print(f" [x] Master received model with accuracy {accuracy} from {method.routing_key}")
    
    if reply_count >= expected_replies:
        ch.stop_consuming()

def consume_replies():
    """Consume replies from workers."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_consume(queue='master_reply_queue', on_message_callback=master_callback, auto_ack=True)
    print(" [*] Master waiting for model replies.")
    channel.start_consuming()
    connection.close()

def perform_ensemble_learning():
    """Combine models using ensemble learning and evaluate."""
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    
    # Handle missing values
    imputer = SimpleImputer(strategy='mean')
    X_imputed = imputer.fit_transform(X)
    
    # Create an ensemble model using VotingClassifier
    ensemble_model = VotingClassifier(estimators=[
        ('logistic_regression', received_models[0]),  # Logistic Regression
        ('knn', received_models[1])  # KNeighborsClassifier
    ], voting='soft')  # Can use 'hard' voting as well
    
    # Train the ensemble model
    ensemble_model.fit(X_imputed, y)
    
    # Predict and calculate accuracy
    y_pred = ensemble_model.predict(X_imputed)
    ensemble_accuracy = accuracy_score(y, y_pred)
    
    print(f"\n [*] Ensemble Model Accuracy: {ensemble_accuracy}")

# Setup RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the exchange and reply queue
channel.exchange_declare(exchange='model_training_exchange', exchange_type='direct')
channel.queue_declare(queue='master_reply_queue')
channel.queue_bind(exchange='model_training_exchange', queue='master_reply_queue', routing_key='master_reply')

connection.close()

# Load dataset
data = pd.read_csv("framingham.csv")
data = data[['sysBP', 'glucose', 'age', 'cigsPerDay', 'totChol', 'diaBP', 'prevalentHyp', 'male', 'BPMeds', 'diabetes', 'TenYearCHD']]

# Handle missing values
imputer = SimpleImputer(strategy='mean')
data_imputed = pd.DataFrame(imputer.fit_transform(data), columns=data.columns)

# Prepare models
logistic_regression_model = LogisticRegression(max_iter=1000)
knn_model = KNeighborsClassifier()

# Send data and models to workers
send_task_to_worker('worker_1', data_imputed, logistic_regression_model)  # Send Logistic Regression to Worker 1
send_task_to_worker('worker_2', data_imputed, knn_model)  # Send KNeighborsClassifier to Worker 2

# Start consuming replies
consuming_thread = threading.Thread(target=consume_replies)
consuming_thread.start()
consuming_thread.join()

# Perform ensemble learning after receiving all models
if len(received_models) == expected_replies:
    perform_ensemble_learning()
