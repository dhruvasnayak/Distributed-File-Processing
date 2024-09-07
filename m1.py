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

# Global variables
expected_replies = 2
reply_count = 0
received_models = []
model_accuracies = []

# Create a single connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def deserialize_model(model_serialized):
    """Deserialize a model from a base64-encoded string."""
    model_data = base64.b64decode(model_serialized)
    return load(BytesIO(model_data))

def serialize_model(model):
    """Serialize a model to a base64-encoded string."""
    buffer = BytesIO()
    dump(model, buffer)
    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode('utf-8')

def send_task_to_worker(worker_routing_key, data, model):
    """Send a task with data and model to a specified worker."""
    data_json = data.to_json(orient='split')
    model_serialized = serialize_model(model)
    model_type = 'logistic_regression' if isinstance(model, LogisticRegression) else 'knn'
    
    message = json.dumps({
        'data': data_json,
        'model': model_serialized,
        'model_type': model_type
    })
    
    channel.basic_publish(
        exchange='model_training_exchange',
        routing_key=worker_routing_key,
        body=message
    )

def master_callback(ch, method, properties, body):
    """Callback function to handle replies from workers."""
    global reply_count, received_models, model_accuracies

    reply_count += 1
    response = json.loads(body)
    
    model_serialized = response.get('model')
    accuracy = response.get('accuracy')
    
    if model_serialized is None or accuracy is None:
        return
    
    model = deserialize_model(model_serialized)
    received_models.append(model)
    model_accuracies.append(accuracy)
    
    if reply_count >= expected_replies:
        ch.stop_consuming()

def consume_replies():
    """Set up RabbitMQ consumer for model replies."""
    channel.basic_consume(queue='master_reply_queue', on_message_callback=master_callback, auto_ack=True)
    channel.start_consuming()

def perform_ensemble_learning():
    """Perform ensemble learning using the received models."""
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1]
    
    imputer = SimpleImputer(strategy='mean')
    X_imputed = imputer.fit_transform(X)
    
    ensemble_model = VotingClassifier(
        estimators=[
            ('logistic_regression', received_models[0]),
            ('knn', received_models[1])
        ],
        voting='soft'
    )
    
    ensemble_model.fit(X_imputed, y)
    y_pred = ensemble_model.predict(X_imputed)
    ensemble_accuracy = accuracy_score(y, y_pred)

    # Print final ensemble model accuracy
    print(f"\n[*] Ensemble Model Accuracy: {ensemble_accuracy}")

data = pd.read_csv("framingham.csv")
data = data[['sysBP', 'glucose', 'age', 'cigsPerDay', 'totChol', 'diaBP', 'prevalentHyp', 'male', 'BPMeds', 'diabetes', 'TenYearCHD']]
imputer = SimpleImputer(strategy='mean')
data_imputed = pd.DataFrame(imputer.fit_transform(data), columns=data.columns)

logistic_regression_model = LogisticRegression(max_iter=1000)
knn_model = KNeighborsClassifier()

send_task_to_worker('worker_1', data_imputed, logistic_regression_model)
send_task_to_worker('worker_2', data_imputed, knn_model)

consuming_thread = threading.Thread(target=consume_replies)
consuming_thread.start()
consuming_thread.join()

if len(received_models) == expected_replies:
    perform_ensemble_learning()

# Close the connection
connection.close()
