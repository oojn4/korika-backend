import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import io
import base64
import os
import joblib
from flask import current_app
from tensorflow.keras.models import load_model
from app.ml.model import MultivariateTimeSeriesLSTM
from sqlalchemy.sql import text
from app import db

# Configure matplotlib to use non-interactive backend
matplotlib.use('Agg')

def predict_six_months_ahead(model_instance, trained_model, df, facility_id):
    """
    Predict 6 months ahead using historical data for a specific facility
    """
    # Get historical data for the specified facility (last 6 months)
    facility_data = df[df['id_faskes'] == facility_id].copy()
    if len(facility_data) == 0:
        print(f"No data found for facility ID {facility_id}")
        # Try to find closest facility ID
        available_ids = df['id_faskes'].unique()
        closest_id = available_ids[np.abs(available_ids - facility_id).argmin()]
        print(f"Using closest facility ID {closest_id} instead")
        facility_id = closest_id
        facility_data = df[df['id_faskes'] == facility_id].copy()

    # Create datetime and sort
    facility_data['date'] = pd.to_datetime(
        facility_data['tahun'].astype(str) + '-' +
        facility_data['bulan'].astype(str).str.zfill(2) + '-01'
    )
    facility_data = facility_data.sort_values('date')

    # Get last 6 months of data
    last_six_months = facility_data.tail(6).reset_index(drop=True)
    if len(last_six_months) < 6:
        print(f"Warning: Only {len(last_six_months)} months of data available for facility {facility_id}")
        print("Padding with previous months' data if available")
        # Try to fill with previous months if available
        while len(last_six_months) < 6 and len(facility_data) > len(last_six_months):
            additional_data = facility_data.iloc[-(len(last_six_months)+1)].to_frame().T
            last_six_months = pd.concat([additional_data, last_six_months], ignore_index=True)

    # Calculate start date for predictions
    last_date = last_six_months['date'].max()
    prediction_dates = []
    for i in range(1, 7):
        next_date = last_date + pd.DateOffset(months=i)
        prediction_dates.append(next_date)

    # Feature columns
    feature_columns = [
        'hujan_hujan_mean', 'hujan_hujan_max', 'hujan_hujan_min',
        'tm_tm_mean', 'tm_tm_max', 'tm_tm_min', 'ss_monthly_mean',
        'ff_x_monthly_mean', 'ddd_x_monthly_mean', 'ff_avg_monthly_mean',
        'pop_penduduk_kab', 'pop_penduduk_kec'
    ]

    # Create prediction data (one by one for each month)
    all_predictions = []

    # Use historical window for first prediction
    current_window = last_six_months.copy()

    for i, pred_date in enumerate(prediction_dates):
        # Create prediction data for current month
        pred_data = current_window.copy()

        # Make prediction
        predictions = model_instance.make_predictions(trained_model, pred_data)

        # Create prediction result
        pred_result = {
            'date': pred_date,
            'tahun': pred_date.year,
            'bulan': pred_date.month,
            'id_faskes': facility_id
        }

        # Add target predictions
        target_columns = [
            'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt',
            'prop_kab_pos_0_4', 'prop_kab_pos_5_14', 'prop_kab_pos_15_64',
            'prop_kab_pos_diatas_64', 'prop_kab_kematian_malaria', 'prop_kab_hamil_pos',
            'prop_kec_pos_0_4', 'prop_kec_pos_5_14', 'prop_kec_pos_15_64',
            'prop_kec_pos_diatas_64', 'prop_kec_kematian_malaria', 'prop_kec_hamil_pos',
            'obat_standar', 'obat_nonprogram', 'obat_primaquin'
        ]

        for j, col in enumerate(target_columns):
            pred_result[col] = float(predictions[0][j])  # Convert to float for JSON serialization

        all_predictions.append(pred_result)

        # Update window for next prediction:
        # 1. Remove oldest month
        # 2. Add new prediction as the newest month
        new_pred_record = pred_data.iloc[-1].copy()
        new_pred_record['date'] = pred_date
        new_pred_record['tahun'] = pred_date.year
        new_pred_record['bulan'] = pred_date.month

        # Copy predicted values
        for j, col in enumerate(target_columns):
            new_pred_record[col] = predictions[0][j]

        # Remove oldest record and add newest
        current_window = current_window.iloc[1:].copy()
        current_window = pd.concat([current_window, pd.DataFrame([new_pred_record])], ignore_index=True)

    # Combine all predictions
    predictions_df = pd.DataFrame(all_predictions)
    
    return predictions_df, facility_id

def generate_prediction_plots(predictions):
    """Generate visualization plots for predictions"""
    fig = plt.figure(figsize=(15, 10))
    
    # Plot konfirmasi lab
    plt.subplot(2, 2, 1)
    plt.plot(predictions['date'], predictions['konfirmasi_lab_mikroskop'], 'o-', label='Mikroskop')
    plt.plot(predictions['date'], predictions['konfirmasi_lab_rdt'], 's-', label='RDT')
    plt.title('Prediksi Konfirmasi Lab')
    plt.xlabel('Bulan')
    plt.ylabel('Jumlah Kasus')
    plt.legend()
    plt.grid(True)

    # Plot proporsi kab
    plt.subplot(2, 2, 2)
    plt.plot(predictions['date'], predictions['prop_kab_pos_0_4'], 'o-', label='0-4 tahun')
    plt.plot(predictions['date'], predictions['prop_kab_pos_5_14'], 's-', label='5-14 tahun')
    plt.plot(predictions['date'], predictions['prop_kab_pos_15_64'], '^-', label='15-64 tahun')
    plt.plot(predictions['date'], predictions['prop_kab_pos_diatas_64'], 'd-', label='>64 tahun')
    plt.title('Prediksi Proporsi Positif Kabupaten')
    plt.xlabel('Bulan')
    plt.ylabel('Proporsi')
    plt.legend()
    plt.grid(True)

    # Plot proporsi kec
    plt.subplot(2, 2, 3)
    plt.plot(predictions['date'], predictions['prop_kec_pos_0_4'], 'o-', label='0-4 tahun')
    plt.plot(predictions['date'], predictions['prop_kec_pos_5_14'], 's-', label='5-14 tahun')
    plt.plot(predictions['date'], predictions['prop_kec_pos_15_64'], '^-', label='15-64 tahun')
    plt.plot(predictions['date'], predictions['prop_kec_pos_diatas_64'], 'd-', label='>64 tahun')
    plt.title('Prediksi Proporsi Positif Kecamatan')
    plt.xlabel('Bulan')
    plt.ylabel('Proporsi')
    plt.legend()
    plt.grid(True)

    # Plot obat
    plt.subplot(2, 2, 4)
    plt.plot(predictions['date'], predictions['obat_standar'], 'o-', label='Standar')
    plt.plot(predictions['date'], predictions['obat_nonprogram'], 's-', label='Non-program')
    plt.plot(predictions['date'], predictions['obat_primaquin'], '^-', label='Primaquin')
    plt.title('Prediksi Kebutuhan Obat')
    plt.xlabel('Bulan')
    plt.ylabel('Jumlah')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    
    # Convert plot to PNG image
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()
    plt.close(fig)
    
    return plot_url

def get_model_data_from_db():
    """Query database to get data needed for model training"""
    # Sesuaikan query agar sesuai dengan struktur data Excel terbaru
    query = text("""
    WITH weather_data AS (
        SELECT 
            mhfm.id_faskes,
            mhfm.bulan,
            mhfm.tahun,
            AVG(wd.hujan_mean) as hujan_hujan_mean,
            MAX(wd.hujan_max) as hujan_hujan_max,
            MIN(wd.hujan_min) as hujan_hujan_min,
            AVG(wd.tm_mean) as tm_tm_mean,
            MAX(wd.tm_max) as tm_tm_max,
            MIN(wd.tm_min) as tm_tm_min,
            AVG(wd.ss_mean) as ss_monthly_mean,
            AVG(wd.ff_x_mean) as ff_x_monthly_mean,
            AVG(wd.ddd_x_mean) as ddd_x_monthly_mean,
            AVG(wd.ff_avg) as ff_avg_monthly_mean
        FROM 
            malaria_health_facility_monthly mhfm
        JOIN 
            health_facility_id hfi ON mhfm.id_faskes = hfi.id_faskes
        LEFT JOIN 
            weather_data wd ON wd.kabupaten = hfi.kabupaten AND 
                            wd.bulan = mhfm.bulan AND 
                            wd.tahun = mhfm.tahun
        GROUP BY
            mhfm.id_faskes, mhfm.bulan, mhfm.tahun
    ),
    population_data AS (
        SELECT 
            mhfm.id_faskes,
            mhfm.bulan,
            mhfm.tahun,
            pd.population_kab as pop_penduduk_kab,
            pd.population_kec as pop_penduduk_kec
        FROM 
            malaria_health_facility_monthly mhfm
        JOIN 
            health_facility_id hfi ON mhfm.id_faskes = hfi.id_faskes
        LEFT JOIN 
            population_data pd ON pd.kabupaten = hfi.kabupaten AND
                                pd.kecamatan = hfi.kecamatan AND
                                pd.tahun = mhfm.tahun
        GROUP BY
            mhfm.id_faskes, mhfm.bulan, mhfm.tahun, pd.population_kab, pd.population_kec
    )
    SELECT 
        mhfm.id_faskes,
        mhfm.bulan,
        mhfm.tahun,
        -- Semua kolom yang diperlukan untuk model
        -- Sesuaikan berdasarkan data Excel terbaru
        ...
    FROM 
        malaria_health_facility_monthly mhfm
    JOIN 
        weather_data w ON mhfm.id_faskes = w.id_faskes AND mhfm.bulan = w.bulan AND mhfm.tahun = w.tahun
    JOIN 
        population_data p ON mhfm.id_faskes = p.id_faskes AND mhfm.bulan = p.bulan AND mhfm.tahun = p.tahun
    WHERE
        mhfm.status = 'actual'
    """)
    
    try:
        result = db.session.execute(query).fetchall()
        df = pd.DataFrame(result)
        return df
    except Exception as e:
        print(f"Error getting data from database: {e}")
        return None
def train_or_load_model():
    """Load existing model or train a new one if necessary"""
    model_path = os.path.join(current_app.config['MODELS_FOLDER'], 'best_model.keras')
    scaler_path = os.path.join(current_app.config['MODELS_FOLDER'], 'scalers.joblib')
    
    if os.path.exists(model_path) and os.path.exists(scaler_path):
        # Load model and scalers
        model = load_model(model_path)
        model_instance = MultivariateTimeSeriesLSTM(
            window_len=current_app.config['ML_WINDOW_LENGTH'],
            batch_size=current_app.config['ML_BATCH_SIZE']
        )
        scalers = joblib.load(scaler_path)
        model_instance.feature_scaler = scalers['feature_scaler']
        model_instance.target_scaler = scalers['target_scaler']
        model_instance.facility_encoder = scalers['facility_encoder']
        print("Loaded existing model and scalers")
        return model, model_instance, True
    else:
        # Get data for model training
        df = get_model_data_from_db()
        if df is None or len(df) == 0:
            print("No data available for model training")
            return None, None, False
            
        # Train new model
        print("Training new model...")
        
        # Train model
        model_instance = MultivariateTimeSeriesLSTM(
            window_len=current_app.config['ML_WINDOW_LENGTH'],
            batch_size=current_app.config['ML_BATCH_SIZE']
        )
        model, _, n_features, n_targets = model_instance.train_model(df, epochs=current_app.config['ML_EPOCHS'])  
        
        # Save model and scalers
        model.save(model_path)
        scalers = {
            'feature_scaler': model_instance.feature_scaler,
            'target_scaler': model_instance.target_scaler,
            'facility_encoder': model_instance.facility_encoder
        }
        joblib.dump(scalers, scaler_path)
        print("Trained and saved new model and scalers")
        return model, model_instance, True