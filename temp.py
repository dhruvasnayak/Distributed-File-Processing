# Import necessary libraries
import numpy as np
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC

# Load the Digits dataset
data = load_digits()
X = data.data
y = data.target

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Initialize different models
model1 = LogisticRegression(max_iter=2000)
model2 = SVC(probability=True)
model3 = RandomForestClassifier()
model4 = GradientBoostingClassifier()

# Train the models
model1.fit(X_train, y_train)
model2.fit(X_train, y_train)
model3.fit(X_train, y_train)
model4.fit(X_train, y_train)

# Make predictions with each model
pred1 = model1.predict(X_test)
pred2 = model2.predict(X_test)
pred3 = model3.predict(X_test)
pred4 = model4.predict(X_test)

# Print accuracy of each model
accuracy_lr = accuracy_score(y_test, pred1)
accuracy_svc = accuracy_score(y_test, pred2)
accuracy_rf = accuracy_score(y_test, pred3)
accuracy_gb = accuracy_score(y_test, pred4)

print(f"Logistic Regression Accuracy: {accuracy_lr:.2f}")
print(f"SVC Accuracy: {accuracy_svc:.2f}")
print(f"Random Forest Accuracy: {accuracy_rf:.2f}")
print(f"Gradient Boosting Accuracy: {accuracy_gb:.2f}")

# Combine models using VotingClassifier
voting_clf = VotingClassifier(estimators=[
    ('lr', model1),
    ('svc', model2),
    ('rf', model3),
    ('gb', model4)
], voting='soft')

# Train the voting classifier
voting_clf.fit(X_train, y_train)

# Make predictions with the voting classifier
voting_pred = voting_clf.predict(X_test)

# Print accuracy of the voting classifier
accuracy_voting = accuracy_score(y_test, voting_pred)
print(f"Voting Classifier Accuracy: {accuracy_voting:.2f}")
