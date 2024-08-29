import pika
import json
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from joblib import dump
from sklearn.metrics import accuracy_score
import io
import os
from sklearn.impute import SimpleImputer

# Directory to save model files
MODEL_DIR = 'models'
os.makedirs(MODEL_DIR, exist_ok=True)

def train_model(data):
    """Train a Random Forest Classifier model on the given data."""
    X = data.iloc[:, :-1]  # Features
    y = data.iloc[:, -1]   # Labels
    
    # Impute missing values
    imputer = SimpleImputer(strategy='mean')
    X_imputed = imputer.fit_transform(X)
    
    model = RandomForestClassifier(n_estimators=200, random_state=0, max_depth=12)
    model.fit(X_imputed, y)
    return model

def worker_callback(ch, method, properties, body):
    worker_name = method.routing_key
    message = json.loads(body)
    data_json = message.get('data', None)
    
    if data_json is None:
        print(f" [x] {worker_name} received message without data")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    data = pd.read_json(io.StringIO(data_json), orient='split')

    print(f" [x] {worker_name} received data")

    # Train the model on the entire dataset
    model = train_model(data)
    
    if model is None:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Save the model to a file
    model_file = os.path.join(MODEL_DIR, f'{worker_name}_model.pkl')
    dump(model, model_file)
    
    # Evaluate the model on the same data
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    X_imputed = SimpleImputer(strategy='mean').fit_transform(X)
    y_pred = model.predict(X_imputed)
    accuracy = accuracy_score(y, y_pred)
    
    # Send the model back to the master along with its accuracy
    reply_message = json.dumps({'model_file': model_file, 'accuracy': accuracy})
    ch.basic_publish(exchange='model_training_exchange',
                     routing_key='master_reply',
                     body=reply_message)
    print(f" [x] {worker_name} sent model file {model_file} with accuracy {accuracy}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Setup for a specific worker
worker_name = 'worker_3'
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f'{worker_name}_queue')
channel.queue_bind(exchange='model_training_exchange', queue=f'{worker_name}_queue', routing_key=worker_name)

channel.basic_consume(queue=f'{worker_name}_queue', on_message_callback=worker_callback)

print(f" [*] {worker_name} waiting for tasks.")
channel.start_consuming()
