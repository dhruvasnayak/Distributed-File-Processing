import pika
import json
import pandas as pd
from joblib import dump, load
from sklearn.metrics import accuracy_score
import io
import base64
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier

def serialize_model(model):
    """Serialize and base64 encode the model."""
    buffer = io.BytesIO()
    dump(model, buffer)
    buffer.seek(0)
    model_serialized = base64.b64encode(buffer.read()).decode('utf-8')
    return model_serialized

def deserialize_model(model_serialized):
    """Deserialize a base64 encoded model."""
    model_data = base64.b64decode(model_serialized)
    model = load(io.BytesIO(model_data))
    return model

def train_model(model, data):
    """Train the given model on the data."""
    X = data.iloc[:, :-1]  # Features
    y = data.iloc[:, -1]   # Labels

    # Impute missing values
    imputer = SimpleImputer(strategy='mean')
    X_imputed = imputer.fit_transform(X)
    
    model.fit(X_imputed, y)
    return model

def worker_callback(ch, method, properties, body):
    worker_name = method.routing_key
    message = json.loads(body)
    data_json = message.get('data', None)
    model_serialized = message.get('model', None)
    model_type = message.get('model_type', None)
    
    if data_json is None or model_serialized is None or model_type is None:
        print(f" [x] {worker_name} received incomplete message")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    data = pd.read_json(io.StringIO(data_json), orient='split')

    print(f" [x] {worker_name} received data and model {model_type}")

    # Load the appropriate model
    if model_type == 'logistic_regression':
        model = LogisticRegression()
    elif model_type == 'knn':
        model = KNeighborsClassifier()
    else:
        print(f" [x] {worker_name} received unknown model type: {model_type}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Deserialize the received model
    model = deserialize_model(model_serialized)

    # Train the model on the data
    trained_model = train_model(model, data)

    # Serialize the trained model for response
    model_serialized_response = serialize_model(trained_model)

    # Evaluate the model
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    X_imputed = SimpleImputer(strategy='mean').fit_transform(X)
    y_pred = trained_model.predict(X_imputed)
    accuracy = accuracy_score(y, y_pred)
    
    # Send the serialized model and accuracy back to the master
    reply_message = json.dumps({'model': model_serialized_response, 'accuracy': accuracy})
    ch.basic_publish(exchange='model_training_exchange',
                     routing_key='master_reply',
                     body=reply_message)
    print(f" [x] {worker_name} sent model with accuracy {accuracy}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Setup for a specific worker
worker_name = 'worker_2'  # Change for each worker (e.g., 'worker_1', 'worker_2')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue=f'{worker_name}_queue')
channel.queue_bind(exchange='model_training_exchange', queue=f'{worker_name}_queue', routing_key=worker_name)

channel.basic_consume(queue=f'{worker_name}_queue', on_message_callback=worker_callback)

print(f" [*] {worker_name} waiting for tasks.")
channel.start_consuming()
