from flask import Blueprint, request, jsonify, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
import os
from app.models.db_models import MalariaHealthFacilityMonthly
from app.ml.utils import train_or_load_model, get_model_data_from_db, predict_six_months_ahead, generate_prediction_plots
from app import db
from flask import current_app

bp = Blueprint('prediction', __name__, url_prefix='')

@bp.route('/train-model', methods=['POST'])
@jwt_required()
def train_model():
    """Train or retrain the ML model"""
    identity = get_jwt_identity()
    if identity.get('access_level') != 'admin':
        return jsonify({"error": "You do not have permission to train models"}), 403
        
    try:
        # Force retraining by deleting existing model files
        model_path = os.path.join(current_app.config['MODELS_FOLDER'], 'best_model.keras')
        scaler_path = os.path.join(current_app.config['MODELS_FOLDER'], 'scalers.joblib')
        
        # Remove existing model files if they exist
        if os.path.exists(model_path):
            os.remove(model_path)
        if os.path.exists(scaler_path):
            os.remove(scaler_path)
            
        # Train new model
        model, model_instance, success = train_or_load_model()
        
        if not success:
            return jsonify({
                "success": False,
                "message": "Failed to train model. Check logs for details."
            }), 500
            
        return jsonify({
            "success": True,
            "message": "Model trained successfully"
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@bp.route('/predict', methods=['POST'])
@jwt_required()
def predict():
    """Generate 6-month predictions for a facility"""
    try:
        data = request.json
        facility_id = data.get('facility_id')
        
        if not facility_id:
            return jsonify({"error": "Facility ID is required"}), 400
            
        # Load or train model
        model, model_instance, success = train_or_load_model()
        
        if not success:
            return jsonify({
                "success": False,
                "message": "Model not available. Please train the model first."
            }), 500
            
        # Get data for prediction
        df = get_model_data_from_db()
        
        if df is None or len(df) == 0:
            return jsonify({
                "success": False,
                "message": "No data available for prediction"
            }), 500
            
        # Make predictions
        predictions_df, actual_facility_id = predict_six_months_ahead(model_instance, model, df, facility_id)
        
        # Generate plots
        plot_url = generate_prediction_plots(predictions_df)
        
        # Save predictions to CSV
        result_filename = f'prediksi_6_bulan_faskes_{actual_facility_id}.csv'
        result_path = os.path.join(current_app.config['RESULT_FOLDER'], result_filename)
        predictions_df.to_csv(result_path, index=False)
        
        # Insert predictions into database
        insert_predictions_to_db(predictions_df)
        
        return jsonify({
            "success": True,
            "facility_id": actual_facility_id,
            "predictions": predictions_df.to_dict('records'),
            "plot_url": plot_url,
            "filename": result_filename
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@bp.route('/download-prediction/<filename>', methods=['GET'])
@jwt_required()
def download_prediction(filename):
    """Download prediction CSV file"""
    try:
        return send_file(
            os.path.join(current_app.config['RESULT_FOLDER'], filename),
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def insert_predictions_to_db(predictions_df):
    """Insert prediction results into database"""
    try:
        for _, row in predictions_df.iterrows():
            # Convert prediction values to database format
            prediction = MalariaHealthFacilityMonthly(
                id_faskes=int(row['id_faskes']),
                bulan=int(row['bulan']),
                tahun=int(row['tahun']),
                konfirmasi_lab_mikroskop=int(round(row['konfirmasi_lab_mikroskop'])),
                konfirmasi_lab_rdt=int(round(row['konfirmasi_lab_rdt'])),
                obat_standar=int(round(row['obat_standar'])),
                obat_nonprogram=int(round(row['obat_nonprogram'])),
                obat_primaquin=int(round(row['obat_primaquin'])),
                status="predicted"
            )
            
            # Check if prediction already exists
            existing = MalariaHealthFacilityMonthly.query.filter_by(
                id_faskes=prediction.id_faskes,
                bulan=prediction.bulan,
                tahun=prediction.tahun,
                status="predicted"
            ).first()
            
            if existing:
                # Update existing prediction
                existing.konfirmasi_lab_mikroskop = prediction.konfirmasi_lab_mikroskop
                existing.konfirmasi_lab_rdt = prediction.konfirmasi_lab_rdt
                existing.obat_standar = prediction.obat_standar
                existing.obat_nonprogram = prediction.obat_nonprogram
                existing.obat_primaquin = prediction.obat_primaquin
            else:
                # Add new prediction
                db.session.add(prediction)
                
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        print(f"Error inserting predictions to database: {e}")
        return False