import csv
from flask import Blueprint, request, jsonify, send_file
from flask_jwt_extended import jwt_required, get_jwt_identity
import os
from app.models.db_models import MalariaHealthFacilityMonthly
from app.ml.utils import train_or_load_model, get_model_data_from_db, predict_six_months_ahead, generate_prediction_plots
from app import db
from flask import current_app
from datetime import datetime
import pandas as pd

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
        
        identity = get_jwt_identity()
        if identity.get('access_level') != 'admin':
            return jsonify({"error": "You do not have permission to perform this action"}), 403
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
        print(df)
        if df is None or len(df) == 0:
            return jsonify({
                "success": False,
                "message": "No data available for prediction"
            }), 500
            
        # Make predictions
        predictions_df, actual_facility_id = predict_six_months_ahead(model_instance, model, df, facility_id)
        print("hfi")
        print(actual_facility_id)
        # Generate plots
        # plot_url = generate_prediction_plots(predictions_df)
        
        # Save predictions to CSV
        # result_filename = f'prediksi_6_bulan_faskes_{actual_facility_id}.csv'
        # result_path = os.path.join(current_app.config['RESULT_FOLDER'], result_filename)
        # predictions_df.to_csv(result_path, index=False)
        
        # Insert predictions into database
        insert_predictions_to_db(predictions_df)
        
        return jsonify({
            "success": True,
            "facility_id": actual_facility_id,
            "predictions": predictions_df.to_dict('records'),
            # "plot_url": plot_url,
            # "filename": result_filename
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
        print(predictions_df)
        for _, row in predictions_df.iterrows():
            # Convert prediction values to database format

            prediction = MalariaHealthFacilityMonthly(
                id_faskes=int(row['id_faskes']),
                bulan=int(row['bulan']),
                tahun=int(row['tahun']),
                total_konfirmasi_lab = int(round(row['konfirmasi_lab_mikroskop'])) + int(round(row['konfirmasi_lab_rdt'])) + int(round(row['konfirmasi_lab_pcr'])),
                tot_pos = int(round(row['pos_0_4'])) + int(round(row['pos_5_14'])) + int(round(row['pos_15_64'])) + int(round(row['pos_diatas_64'])),
                konfirmasi_lab_mikroskop=int(round(row['konfirmasi_lab_mikroskop'])),
                konfirmasi_lab_rdt=int(round(row['konfirmasi_lab_rdt'])),
                konfirmasi_lab_pcr=int(round(row['konfirmasi_lab_pcr'])),
                pos_0_4=int(round(row['pos_0_4'])),
                pos_5_14=int(round(row['pos_5_14'])),
                pos_15_64=int(round(row['pos_15_64'])),
                pos_diatas_64=int(round(row['pos_diatas_64'])),
                hamil_pos=int(round(row['hamil_pos'])),
                kematian_malaria=int(round(row['kematian_malaria'])),
                obat_standar=int(round(row['obat_standar'])),
                obat_nonprogram=int(round(row['obat_nonprogram'])),
                obat_primaquin=int(round(row['obat_primaquin'])),
                p_pf=int(round(row['p_pf'])),
                p_pv=int(round(row['p_pv'])),
                p_po=int(round(row['p_po'])),
                p_pm=int(round(row['p_pm'])),
                p_pk=int(round(row['p_pk'])),
                p_mix=int(round(row['p_mix'])),
                p_suspek_pk=int(round(row['p_suspek_pk'])),
                kasus_pe=int(round(row['kasus_pe'])),
                penularan_indigenus=int(round(row['penularan_indigenus'])),
                penularan_impor=int(round(row['penularan_impor'])),
                penularan_induced=int(round(row['penularan_induced'])),
                relaps=int(round(row['relaps'])),
                status="predicted")
            
            # Check if prediction already exists
            existing = MalariaHealthFacilityMonthly.query.filter_by(
                id_faskes=prediction.id_faskes,
                bulan=prediction.bulan,
                tahun=prediction.tahun,
                status="predicted"
            ).first()
            
            if existing:
                existing.konfirmasi_lab_mikroskop=prediction.konfirmasi_lab_mikroskop,
                existing.konfirmasi_lab_rdt=prediction.konfirmasi_lab_rdt,
                existing.konfirmasi_lab_pcr=prediction.konfirmasi_lab_pcr,
                existing.total_konfirmasi_lab=prediction.total_konfirmasi_lab,
                existing.tot_pos=prediction.tot_pos,
                existing.pos_0_4=prediction.pos_0_4,
                existing.pos_5_14=prediction.pos_5_14,
                existing.pos_15_64=prediction.pos_15_64,
                existing.pos_diatas_64=prediction.pos_diatas_64,
                existing.hamil_pos=prediction.hamil_pos,
                existing.kematian_malaria=prediction.kematian_malaria,
                existing.obat_standar=prediction.obat_standar,
                existing.obat_nonprogram=prediction.obat_nonprogram,
                existing.obat_primaquin=prediction.obat_primaquin,
                existing.p_pf=prediction.p_pf,
                existing.p_pv=prediction.p_pv,
                existing.p_po=prediction.p_po,
                existing.p_pm=prediction.p_pm,
                existing.p_pk=prediction.p_pk,
                existing.p_mix=prediction.p_mix,
                existing.p_suspek_pk=prediction.p_suspek_pk,
                existing.kasus_pe=prediction.kasus_pe,
                existing.penularan_indigenus=prediction.penularan_indigenus,
                existing.penularan_impor=prediction.penularan_impor,
                existing.penularan_induced=prediction.penularan_induced,
                existing.relaps=prediction.relaps,
            else:
                # Add new prediction
                db.session.add(prediction)
                
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        print(f"Error inserting predictions to database: {e}")
        return False
    finally:
        db.session.close()
    
@bp.route('/predict-all', methods=['POST'])
@jwt_required()
def predict_all_facilities():
    """Generate 6-month predictions for all facilities"""
    try:
        # Load or train model
        identity = get_jwt_identity()
        if identity.get('access_level') != 'admin':
            return jsonify({"error": "You do not have permission to perform this action"}), 403
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
        
        # Get unique facility IDs
        unique_facility_ids = df['id_faskes'].unique().tolist()
        
        if not unique_facility_ids:
            return jsonify({
                "success": False,
                "message": "No health facilities found in the database"
            }), 404
        
        all_predictions = []
        failed_facilities = []
        all_predictions_df = pd.DataFrame()
        # Make predictions for each facility
        for facility_id in unique_facility_ids:
            try:
                print(f"Predicting for facility {facility_id}")
                predictions_df, actual_facility_id = predict_six_months_ahead(model_instance, model, df, facility_id)
                
                # Save predictions to CSV
                # result_filename = f'prediksi_6_bulan_faskes_{actual_facility_id}.csv'
                # result_path = os.path.join(current_app.config['RESULT_FOLDER'], result_filename)
                # predictions_df.to_csv(result_path, index=False)
                
                # Insert predictions into database
                success = insert_predictions_to_db(predictions_df)
                
                if success:
                    all_predictions_df = pd.concat([all_predictions_df, predictions_df])
                    all_predictions.append({
                        "facility_id": facility_id,
                        "predictions_count": len(predictions_df),
                        # "filename": result_filename
                    })
                else:
                    failed_facilities.append(facility_id)
                    
            except Exception as e:
                print(f"Error predicting for facility {facility_id}: {str(e)}")
                failed_facilities.append(facility_id)
                continue
        
        # Generate summary report
        summary_filename = f'prediksi_semua_faskes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        summary_path = os.path.join(current_app.config['RESULT_FOLDER'], summary_filename)
        
        with open(summary_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['facility_id', 'status'])
            
            for pred in all_predictions:
                writer.writerow([pred['facility_id'], 'success'])
            
            for facility_id in failed_facilities:
                writer.writerow([facility_id, 'failed'])
        
        return jsonify({
            "success": True,
            "total_facilities": len(unique_facility_ids),
            "successful_predictions": len(all_predictions),
            "failed_predictions": len(failed_facilities),
            "failed_facilities": failed_facilities,
            "summary_filename": summary_filename,   
            "predictions": all_predictions_df.to_dict('records')
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500