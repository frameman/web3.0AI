import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler


from keras.models import Model
from keras.layers import Input, Dense

def read():
    file = "<Path of combined_transactions.csv >"
    # Load transaction data
    transactions = pd.read_csv(file)
    for i in transactions.columns:
        print(i)


data_need = [
"gas","gasPrice","value"
,"transaction_repeat_within_1_seconds"
,"transaction_repeat_within_1_frequency"
,"transaction_repeat_within_300_seconds"
,"transaction_repeat_within_300_frequency"
,"transaction_repeat_within_1200_seconds"
,"transaction_repeat_within_1200_frequency"
,"transaction_repeat_within_3600_seconds"
,"transaction_repeat_within_3600_frequency"
,"transaction_repeat_within_7200_seconds"
,"transaction_repeat_within_7200_frequency"
,"transaction_repeat_within_18000_seconds"
,"transaction_repeat_within_18000_frequency"
,"transaction_repeat_within_36000_seconds"
,"transaction_repeat_within_36000_frequency"
,"transaction_repeat_within_86400_seconds"
,"transaction_repeat_within_86400_frequency"
,"transaction_repeat_within_172800_seconds"
,"transaction_repeat_within_172800_frequency"
,"transaction_repeat_within_432000_seconds"
,"transaction_repeat_within_432000_frequency"
,"transaction_repeat_within_864000_seconds"
,"transaction_repeat_within_864000_frequency"
,"transaction_repeat_within_2592000_seconds"
,"transaction_repeat_within_2592000_frequency"
,"transaction_repeat_function_within_1_frequency"
,"transaction_repeat_function_within_300_frequency"
,"transaction_repeat_function_within_1200_frequency"
,"transaction_repeat_function_within_3600_frequency"
,"transaction_repeat_function_within_7200_frequency"
,"transaction_repeat_function_within_18000_frequency"
,"transaction_repeat_function_within_36000_frequency"
,"transaction_repeat_function_within_86400_frequency"
,"transaction_repeat_function_within_172800_frequency"
,"transaction_repeat_function_within_432000_frequency"
,"transaction_repeat_function_within_864000_frequency"
,"transaction_repeat_function_within_2592000_frequency"
]

def auto(df):
    df =df.copy()
    # Feature selection
    features = df[[data_need]]

    # Scale features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    # Define the Autoencoder model
    input_dim = features_scaled.shape[1]
    input_layer = Input(shape=(input_dim,))
    encoded = Dense(16, activation='relu')(input_layer)
    encoded = Dense(8, activation='relu')(encoded)
    encoded = Dense(4, activation='relu')(encoded)
    decoded = Dense(8, activation='relu')(encoded)
    decoded = Dense(16, activation='relu')(decoded)
    decoded = Dense(input_dim, activation='sigmoid')(decoded)

    autoencoder = Model(input_layer, decoded)
    autoencoder.compile(optimizer='adam', loss='mean_squared_error')

    # Train the model
    autoencoder.fit(features_scaled, features_scaled, epochs=100, batch_size=32, shuffle=True, validation_split=0.1, verbose=1)

    # Use the trained autoencoder to find reconstruction errors
    predictions = autoencoder.predict(features_scaled)
    mse = np.mean(np.power(features_scaled - predictions, 2), axis=1)
    df['anomaly'] = mse > np.percentile(mse, 99)

    # Flag suspicious transactions
    suspicious_transactions = df[df['anomaly']]
    print(suspicious_transactions)

def main():
    read()

if __name__ == '__main__':
    main()