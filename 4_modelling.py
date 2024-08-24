import sqlite3

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.metrics import confusion_matrix, precision_score
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn import svm
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.neural_network import MLPClassifier, MLPRegressor

# scoring function for regression


def score_regression(model):
    score = 0
    for circuit in df[df.season == 2019]['round'].unique():

        test = df[(df.season == 2019) & (df['round'] == circuit)]
        X_test = test.drop(['driver', 'podium'], axis = 1)
        y_test = test.podium

        #scaling
        X_test = pd.DataFrame(scaler.transform(X_test), columns = X_test.columns)

        # make predictions
        prediction_df = pd.DataFrame(model.predict(X_test), columns = ['results'])
        prediction_df['podium'] = y_test.reset_index(drop = True)
        prediction_df['actual'] = prediction_df.podium.map(lambda x: 1 if x == 1 else 0)
        prediction_df.sort_values('results', ascending = True, inplace = True)
        prediction_df.reset_index(inplace = True, drop = True)
        prediction_df['predicted'] = prediction_df.index
        prediction_df['predicted'] = prediction_df.predicted.map(lambda x: 1 if x == 0 else 0)

        score += precision_score(prediction_df.actual, prediction_df.predicted)

    model_score = score / df[df.season == 2019]['round'].unique().max()
    return model_score


# scoring function for classification

def score_classification(model):
    score = 0
    for circuit in df[df.season == 2019]['round'].unique():

        test = df[(df.season == 2019) & (df['round'] == circuit)]
        X_test = test.drop(['driver', 'podium'], axis = 1)
        y_test = test.podium

        #scaling
        X_test = pd.DataFrame(scaler.transform(X_test), columns = X_test.columns)

        # make predictions
        prediction_df = pd.DataFrame(model.predict_proba(X_test), columns = ['proba_0', 'proba_1'])
        prediction_df['actual'] = y_test.reset_index(drop = True)
        prediction_df.sort_values('proba_1', ascending = False, inplace = True)
        prediction_df.reset_index(inplace = True, drop = True)
        prediction_df['predicted'] = prediction_df.index
        prediction_df['predicted'] = prediction_df.predicted.map(lambda x: 1 if x == 0 else 0)

        score += precision_score(prediction_df.actual, prediction_df.predicted)

    model_score = score / df[df.season == 2019]['round'].unique().max()
    return model_score


connection = sqlite3.connect('races.db')
# Replace 'your_table_name' with the correct table name in your SQLite database
table_name = 'i_df_dum_table'
# SQL query to select all rows from the table with quotes around the table name
query = f'SELECT * FROM "{table_name}"'
# Use pd.read_sql_query() to read the table into a DataFrame
data = pd.read_sql_query(query, connection)
connection.close()

data = data.reset_index(drop=True)

df = data.copy()
df.podium = df.podium.map(lambda x: 1 if x == 1 else 0)

train = df[df.season < 2019]
X_train = train.drop(['driver', 'podium'], axis = 1)
y_train = train.podium

scaler = StandardScaler()
X_train = pd.DataFrame(scaler.fit_transform(X_train), columns = X_train.columns)

# Store the best parameters and scores
best_params = None
best_score = 0

params = {'hidden_layer_sizes': [(80, 20, 40, 5), (75, 25, 50, 10)],
          'activation': ['identity'],
          'solver': ['lbfgs'],
          'alpha': np.logspace(-4, 2, 20)}

for hidden_layer_sizes in params['hidden_layer_sizes']:
    for activation in params['activation']:
        for solver in params['solver']:
            for alpha in params['alpha']:
                model_params = (hidden_layer_sizes, activation, solver, alpha)
                model = MLPClassifier(hidden_layer_sizes=hidden_layer_sizes,
                                      activation=activation, solver=solver, alpha=alpha, random_state=1)
                model.fit(X_train, y_train)

                model_score = score_classification(model)

                print(f"Testing model with parameters: {model_params}")
                print(f"Model Score: {model_score}")

                # Check if the current model has a better score
                if model_score > best_score:
                    best_score = model_score
                    best_params = model_params

# Create a DataFrame to store the results
results_df = pd.DataFrame(columns=['Model', 'Parameters', 'Score'])
results_df = results_df.append({'Model': 'MLPClassifier', 'Parameters': best_params, 'Score': best_score}, ignore_index=True)

# Display the results
print("\nBest Model Parameters:", best_params)
print("Best Model Score:", best_score)

# Plot a chart to visualize the results
plt.figure(figsize=(8, 6))
plt.barh(results_df['Model'], results_df['Score'], color='skyblue')
plt.xlabel('Score')
plt.title('Best Model Performance')
plt.xlim(0, 1)
plt.gca().invert_yaxis()
plt.show()