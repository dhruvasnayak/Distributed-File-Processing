import pika
import json
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from joblib import dump
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler
import io
import os

# Directory to save model files
MODEL_DIR = 'models'
os.makedirs(MODEL_DIR, exist_ok=True)

def train_model(data):
    """Train a Decision Tree model on the given data."""
    X = data.iloc[:, :-1]  # Features
    y = data.iloc[:, -1]   # Labels
    
    # Scale the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    model = DecisionTreeClassifier()  # Use Decision Tree Classifier
    model.fit(X_scaled, y)
    return model, scaler

def worker_callback(ch, method, properties, body):
    worker_name = method.routing_key
    message = json.loads(body)
    chunk_id = message.get('chunk_id', 'unknown')
    data_chunk_json = message.get('data_chunk', None)
    
    if data_chunk_json is None:
        print(f" [x] {worker_name} received message without data_chunk")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    data_chunk = pd.read_json(io.StringIO(data_chunk_json), orient='split')

    print(f" [x] {worker_name} received data chunk {chunk_id}")

    # Train the model on the data chunk
    model, scaler = train_model(data_chunk)
    
    if model is None:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    # Save the model and scaler to files
    model_file = os.path.join(MODEL_DIR, f'{worker_name}_model_{chunk_id}.pkl')
    dump(model, model_file)
    
    scaler_file = os.path.join(MODEL_DIR, f'{worker_name}_scaler_{chunk_id}.pkl')
    dump(scaler, scaler_file)
    
    # Evaluate the model on the same data
    X = data_chunk.iloc[:, :-1]
    y = data_chunk.iloc[:, -1]
    X_scaled = scaler.transform(X)  # Scale the features before prediction
    y_pred = model.predict(X_scaled)
    accuracy = accuracy_score(y, y_pred)
    
    # Send the model back to the master along with its accuracy
    reply_message = json.dumps({'model_file': model_file, 'accuracy': accuracy})
    ch.basic_publish(exchange='model_training_exchange',
                     routing_key='master_reply',
                     body=reply_message)
    print(f" [x] {worker_name} sent model file {model_file} with accuracy {accuracy}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Setup for a specific worker
worker_name = 'worker_2'  # Change for each worker (e.g., 'worker_2')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f'{worker_name}_queue')
channel.queue_bind(exchange='model_training_exchange', queue=f'{worker_name}_queue', routing_key=worker_name)

channel.basic_consume(queue=f'{worker_name}_queue', on_message_callback=worker_callback)

print(f" [*] {worker_name} waiting for tasks.")
channel.start_consuming()
