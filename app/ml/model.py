import tensorflow as tf
from tensorflow.keras.models import Model, load_model
from tensorflow.keras.layers import Input, LSTM, Dense, Dropout, BatchNormalization
from tensorflow.keras.regularizers import l1_l2
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.preprocessing.sequence import TimeseriesGenerator
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, ModelCheckpoint
from sklearn.preprocessing import StandardScaler, LabelEncoder
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import os
import joblib
from flask import config, current_app

class MultivariateTimeSeriesLSTM:
    def __init__(self, window_len=current_app.config['ML_WINDOW_LENGTH'], batch_size=current_app.config['ML_BATCH_SIZE']):
        self.window_len = window_len
        self.batch_size = batch_size
        self.feature_scaler = StandardScaler()
        self.target_scaler = StandardScaler()
        self.facility_encoder = LabelEncoder()
        
    def prepare_data(self, df):
        """
        Prepare data with facility encoding and TimeseriesGenerator
        """
        feature_columns = [
            'hujan_hujan_mean','hujan_hujan_max', 'hujan_hujan_min',
            'tm_tm_mean', 'tm_tm_max', 'tm_tm_min','ss_monthly_mean', 'rh_mean','rh_min','rh_max',
            'ff_x_monthly_mean', 'ddd_x_monthly_mean', 'ff_avg_monthly_mean',
            'pop_penduduk_kab'
        ]

        target_columns = [
            'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
       'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos',
       'kematian_malaria', 'obat_standar', 'obat_nonprogram', 'obat_primaquin',
       'p_pf', 'p_pv', 'p_po', 'p_pm', 'p_pk', 'p_mix', 'p_suspek_pk', 'kasus_pe',
       'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps'
        ]

        # Create datetime and sort
        df['date'] = pd.to_datetime(df['year'].astype(str) + '-' +
                                  df['month'].astype(str).str.zfill(2) + '-01')
        df = df.sort_values(['id_faskes', 'date'])

        # Fit the facility encoder
        self.facility_encoder.fit(df['id_faskes'])

        # Handle missing values
        df[feature_columns] = df[feature_columns].ffill()
        df[feature_columns] = df[feature_columns].bfill()
        df[feature_columns] = df[feature_columns].fillna(0)
        df[target_columns] = df[target_columns].fillna(0)
        df = df.replace([np.inf, -np.inf], 0)

        # Scale features and targets
        X_features = self.feature_scaler.fit_transform(df[feature_columns])
        y = self.target_scaler.fit_transform(df[target_columns])

        # Use features
        X = X_features
        
        return X, y, feature_columns, target_columns

    def build_model(self, n_features, n_targets):
        """
        Build model with modified architecture to prevent gradient issues
        """
        # Time series features input
        ts_input = Input(shape=(self.window_len, n_features))

        # LSTM layers with modified configuration
        x = LSTM(32, return_sequences=True,
                kernel_regularizer=l1_l2(l1=1e-5, l2=1e-5),
                bias_regularizer=l1_l2(l1=1e-5, l2=1e-5),
                activity_regularizer=l1_l2(l1=1e-5, l2=1e-5))(ts_input)
        x = BatchNormalization()(x)
        x = Dropout(0.3)(x)

        x = LSTM(16,
                kernel_regularizer=l1_l2(l1=1e-5, l2=1e-5),
                bias_regularizer=l1_l2(l1=1e-5, l2=1e-5),
                activity_regularizer=l1_l2(l1=1e-5, l2=1e-5))(x)
        x = BatchNormalization()(x)
        x = Dropout(0.3)(x)

        # Dense layers before output
        x = Dense(32, activation='relu',
                kernel_regularizer=l1_l2(l1=1e-5, l2=1e-5))(x)
        x = BatchNormalization()(x)
        x = Dropout(0.3)(x)

        # Output layer
        output = Dense(n_targets, activation='linear')(x)

        # Create model
        model = Model(inputs=ts_input, outputs=output)

        # Compile with modified optimizer settings
        optimizer = tf.keras.optimizers.Adam(
            learning_rate=0.001,
            clipnorm=1.0,
            epsilon=1e-7
        )

        model.compile(
            optimizer=optimizer,
            loss='mse',
            metrics=['mae', 'mse']
        )

        return model

    def train_model(self, df, epochs=current_app.config['ML_EPOCHS']):
        """
        Train model using generators
        """
        # Prepare data
        X, y, feature_columns, target_columns = self.prepare_data(df)
        
        # Split data by time
        train_start = pd.to_datetime('2019-01-01')
        train_end = pd.to_datetime('2019-12-31')
        val_start = pd.to_datetime('2020-01-01')
        val_end = pd.to_datetime('2022-09-30')

        train_mask = (df['date'] >= train_start) & (df['date'] <= train_end)
        val_mask = (df['date'] >= val_start) & (df['date'] <= val_end)
        
        X_train = X[train_mask]
        y_train = y[train_mask]
        X_val = X[val_mask]
        y_val = y[val_mask]
        # Create generators
        train_gen = TimeseriesGenerator(
            data=X_train,
            targets=y_train,
            length=self.window_len,
            sampling_rate=1,
            batch_size=self.batch_size,
            shuffle=False
        )

        val_gen = TimeseriesGenerator(
            data=X_val,
            targets=y_val,
            length=self.window_len,
            sampling_rate=1,
            batch_size=self.batch_size,
            shuffle=False
        )

        # Get dimensions
        n_features = X_train.shape[1]
        n_targets = y_train.shape[1]
        # Build model
        model = self.build_model(n_features, n_targets)

        # Callbacks
        callbacks = [
            tf.keras.callbacks.EarlyStopping(
                monitor='val_loss',
                patience=10,
                restore_best_weights=True
            ),
            tf.keras.callbacks.ReduceLROnPlateau(
                monitor='val_loss',
                factor=0.5,
                patience=5,
                min_lr=0.0001
            ),
            tf.keras.callbacks.ModelCheckpoint(
                os.path.join(current_app.config['MODELS_FOLDER'], 'best_model.keras'),
                monitor='val_loss',
                save_best_only=True
            )
        ]

        # Train
        history = model.fit(
            train_gen,
            epochs=epochs,
            validation_data=val_gen,
            callbacks=callbacks,
            verbose=1
        )

        return model, history, n_features, n_targets
    def make_predictions(self, model, new_data):
        """
        Make predictions for any facility
        """
        # Prepare features
        feature_columns = [
            'hujan_hujan_mean','hujan_hujan_max', 'hujan_hujan_min',
            'tm_tm_mean', 'tm_tm_max', 'tm_tm_min','ss_monthly_mean', 'rh_mean','rh_min','rh_max',
            'ff_x_monthly_mean', 'ddd_x_monthly_mean', 'ff_avg_monthly_mean',
            'pop_penduduk_kab'
        ]

        # Sort by date
        new_data = new_data.sort_values('date')

        # Scale features
        X_features = self.feature_scaler.transform(new_data[feature_columns])
        
        # Check if we have enough data
        if len(X_features) < self.window_len:
            # Pad the sequence with copies of the first row
            padding_needed = self.window_len - len(X_features)
            first_row = X_features[0:1]
            padding = np.repeat(first_row, padding_needed, axis=0)
            X_features_padded = np.vstack([padding, X_features])
        else:
            X_features_padded = X_features
        
        # Create sequences with padded data if necessary
        X_seq = np.array([X_features_padded[-self.window_len:]])
        
        # Make prediction
        scaled_pred = model.predict(X_seq)

        # Inverse transform
        predictions = self.target_scaler.inverse_transform(scaled_pred)

        return predictions