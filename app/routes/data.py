
from sqlalchemy import text
from flask import Blueprint, current_app, request, jsonify, send_file, send_from_directory
import pandas as pd
import os
from app import db
from app.models.db_models import MalariaHealthFacilityMonthly,HealthFacilityId
from werkzeug.utils import secure_filename
import os
import tempfile
from flask_jwt_extended import jwt_required, get_jwt_identity



bp = Blueprint('data', __name__, url_prefix='')

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@bp.route('/upload', methods=['POST'])
@jwt_required()
def upload_malaria_data():
    # Cek user identity
    current_user_id = get_jwt_identity()
    
    # Validasi request body
    if not request.is_json:
        return jsonify({'error': 'Request harus berupa JSON'}), 400
    
    request_data = request.get_json()
    
    if 'data' not in request_data or not isinstance(request_data['data'], list) or not request_data['data']:
        return jsonify({'error': 'Data tidak ditemukan atau format tidak valid'}), 400
    
    excel_data = request_data['data']
    try:
        # Hasil proses
        result = {
            'health_facility_added': 0,
            'health_facility_existed': 0,
            'malaria_data_added': 0,
            'malaria_data_updated': 0,
            'errors': []
        }
        
        # Proses baris per baris dari data JSON
        for index, row in enumerate(excel_data):
            try:
                # Pastikan kolom yang diperlukan ada
                required_columns = ['id_faskes', 'bulan', 'tahun', 'provinsi', 'kabupaten', 'kecamatan']
                missing_columns = [col for col in required_columns if col not in row]
                
                if missing_columns:
                    result['errors'].append(f'Baris ke-{index+1}: Kolom yang diperlukan tidak ditemukan: {missing_columns}')
                    continue
                
                id_faskes = row['id_faskes']
                bulan = row['bulan']
                tahun = row['tahun']
                
                # Proses HealthFacilityId (pastikan tidak duplikat)
                existing_facility = HealthFacilityId.query.filter_by(
                    id_faskes=id_faskes,
                    provinsi=row['provinsi'],
                    kabupaten=row['kabupaten'],
                    kecamatan=row['kecamatan'],
                    nama_faskes=row.get('nama_faskes')
                ).first()
                
                if not existing_facility:
                    # Tambahkan fasilitas kesehatan baru
                    new_facility = HealthFacilityId(
                        id_faskes=id_faskes,
                        provinsi=row['provinsi'],
                        kabupaten=row['kabupaten'],
                        kecamatan=row['kecamatan'],
                        owner=row.get('owner'),
                        tipe_faskes=row.get('tipe_faskes'),
                        nama_faskes=row.get('nama_faskes'),
                        address=row.get('address'),
                        url=row.get('url'),
                        lat=row.get('lat'),
                        lon=row.get('lon')
                    )
                    
                    db.session.add(new_facility)
                    db.session.flush()  # Flush untuk mendapatkan ID yang baru dibuat
                    result['health_facility_added'] += 1
                else:
                    result['health_facility_existed'] += 1
                
                # Proses MalariaHealthFacilityMonthly (update jika duplikat)
                existing_data = MalariaHealthFacilityMonthly.query.filter_by(
                    id_faskes=id_faskes,
                    bulan=bulan,
                    tahun=tahun
                ).first()
                
                # Siapkan data malaria
                malaria_data = {
                    'id_faskes': id_faskes,
                    'bulan': bulan,
                    'tahun': tahun,
                    'status': 'actual',  # Pastikan status selalu "actual"
                }
                
                # Tambahkan kolom lain jika tersedia di data JSON
                # Add validation for data types
                for column, value in row.items():
                    if column not in ['id_faskes', 'bulan', 'tahun', 'provinsi', 'kabupaten', 'kecamatan',
                                    'owner', 'tipe_faskes', 'nama_faskes', 'address', 'url', 'lat', 'lon']:
                        # Pastikan kolom ada di model MalariaHealthFacilityMonthly
                        if hasattr(MalariaHealthFacilityMonthly, column) and value is not None:
                            # Add type checking here
                            column_type = MalariaHealthFacilityMonthly.__table__.columns[column].type.python_type
                            try:
                                # Cast value to appropriate type
                                if column_type == float:
                                    malaria_data[column] = float(value)
                                elif column_type == int:
                                    malaria_data[column] = int(value)
                                elif column_type == str:
                                    malaria_data[column] = str(value)
                                else:
                                    malaria_data[column] = value
                            except (ValueError, TypeError):
                                # Handle type conversion errors
                                error_msg = f"Error konversi tipe data untuk kolom {column}: nilai '{value}' tidak valid untuk tipe {column_type.__name__}"
                                result['errors'].append(error_msg)
                                continue
                
                if existing_data:
                    # Update data yang sudah ada
                    for key, value in malaria_data.items():
                        setattr(existing_data, key, value)
                    result['malaria_data_updated'] += 1
                else:
                    # Tambahkan data baru
                    new_malaria_data = MalariaHealthFacilityMonthly(**malaria_data)
                    db.session.add(new_malaria_data)
                    result['malaria_data_added'] += 1
                
            except Exception as e:
                error_msg = f"Error pada baris ke-{index+1}: {str(e)}"
                result['errors'].append(error_msg)
        
        # Commit semua perubahan ke database
        db.session.commit()
        
        return jsonify({
            'message': 'Data berhasil diproses',
            'result': result
        }), 200
        
    except Exception as e:
        # Rollback jika terjadi error
        db.session.rollback()
        return jsonify({'error': f'Terjadi kesalahan: {str(e)}'}), 500
    finally:
        db.session.close()
    
@bp.route('/get-provinces', methods=['GET'])
@jwt_required()
def get_provinces():
    try:
        # SQL query untuk mengambil data prediksi
        query = text("""
        SELECT provinsi as province
        FROM 
            health_facility_id
        GROUP BY
            provinsi
        """)
        
        result = db.session.execute(query).fetchall()
        provinces = [row.province for row in result]
        
        return jsonify({"data": provinces,"success":True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()

@bp.route('/get-facilities', methods=['GET'])
@jwt_required()
def get_facilities():
    """Get list of facilities for prediction"""
    try:
        province = request.args.get('province', default=None, type=str)
        kabupaten = request.args.get('kabupaten', default=None, type=str)
        
        query = HealthFacilityId.query
        
        if province and province != 'TOTAL':
            query = query.filter_by(provinsi=province)
        if kabupaten:
            query = query.filter_by(kabupaten=kabupaten)
            
        facilities = query.all()
        
        return jsonify({
            "success": True,
            "facilities": [{
                "id_faskes": f.id_faskes,
                "nama_faskes": f.nama_faskes,
                "tipe_faskes": f.tipe_faskes,
                "provinsi": f.provinsi,
                "kabupaten": f.kabupaten,
                "kecamatan": f.kecamatan
            } for f in facilities]
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
    finally:
        db.session.close()

@bp.route('/get-raw-data', methods=['GET'])
@jwt_required()
def get_raw_data():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        if province == 'TOTAL':
            province = None
        
        # SQL query untuk periode saat ini
        current_query = text("""
            SELECT 
                hfi.provinsi AS province,
                hfi.kabupaten AS city,
                hfi.kecamatan AS district,
                mhfm.tahun AS year,
                mhfm.bulan AS month,
                hfi.id_faskes,
                hfi.nama_faskes,
                hfi.tipe_faskes,
                hfi."owner",
                hfi.lat,
                hfi.lon,
                hfi.url,
                mhfm.tot_pos,
                mhfm.konfirmasi_lab_mikroskop,
                mhfm.konfirmasi_lab_rdt,
                mhfm.konfirmasi_lab_pcr,
                mhfm.pos_0_4,
                mhfm.pos_5_14,
                mhfm.pos_15_64,
                mhfm.pos_diatas_64,
                mhfm.hamil_pos,
                mhfm.kematian_malaria,
                mhfm.obat_standar,
                mhfm.obat_nonprogram,
                mhfm.obat_primaquin,
                mhfm.p_pf,
                mhfm.p_pv,
                mhfm.p_po,
                mhfm.p_pm,
                mhfm.p_pk,
                mhfm.p_mix,
                mhfm.p_suspek_pk,
                mhfm.penularan_indigenus,
                mhfm.penularan_impor,
                mhfm.penularan_induced,
                mhfm.relaps,
                mhfm.status
            FROM 
                malaria_health_facility_monthly mhfm
            JOIN 
                health_facility_id hfi
            ON 
                mhfm.id_faskes = hfi.id_faskes
        """)
        params = {}
        if province:
            current_query = text(str(current_query) + " AND hfi.provinsi = :province")
            params['province'] = province
        # Eksekusi query
        current_data = db.session.execute(current_query, params).fetchall()
        
        # Konversi ke dictionary untuk kemudahan manipulasi
        current_data = [row._asdict() for row in current_data]
        previous_data_index = {
            (row['id_faskes'], row['year'], row['month']): row
            for row in [row for row in current_data]
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        for row in current_data:
            prev_row = previous_data_index.get((row['id_faskes'], row['year'], row['month'] - 1))
            for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr']:
                if prev_row and row[key] and prev_row[key]:
                    row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    row[f'{key}_m_to_m_change'] = None

                yoy_row = previous_data_index.get((row['id_faskes'], row['year'] - 1, row['month']))
                if yoy_row and row[key] and yoy_row[key]:
                    row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    row[f'{key}_y_on_y_change'] = None

            # Tambahkan hasil yang telah dihitung ke dalam final_data
            final_data.append(row)
        # Langkah 1: Cari tahun dan bulan terakhir
        max_year_actual = max(row['year'] for row in final_data if row['status'] == 'actual')
        max_month_actual = max(row['month'] for row in final_data if (row['status'] == 'actual')&(row['year'] == max_year_actual))
        
        if max_month_actual == 12:
            next_month_predicted = 1
            next_year_predicted = max_year_actual+1
        else:
            next_month_predicted = max_month_actual+1
            next_year_predicted = max_year_actual

        # Filter data untuk hanya bulan terakhir dan unik berdasarkan id_faskes
        filtered_data = [row for row in final_data if row['year'] == next_year_predicted and row['month'] == next_month_predicted and row['status']=='predicted']
        unique_data = []
        seen = set()
        for row in filtered_data:
            if row['id_faskes'] not in seen:
                unique_data.append(row)
                seen.add(row['id_faskes'])

        return jsonify({"data": unique_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()

@bp.route('/get-aggregate-data', methods=['GET'])
@jwt_required()
def get_aggregate_data():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        year = request.args.get('year', default=None, type=int)
        month = request.args.get('month', default=None, type=int)

        if not province:
            return jsonify({"error": "Parameter 'province' is required"}), 400

        # SQL query untuk mengambil data prediksi
        query = text("""
        WITH combined_data AS (
            (
                SELECT 
                    'TOTAL' AS province,
                    mhfm.tahun AS year,
                    mhfm.bulan AS month,
                    mhfm.status AS status,
                    SUM(mhfm.tot_pos) AS tot_pos,
                    SUM(mhfm.konfirmasi_lab_mikroskop) as konfirmasi_lab_mikroskop,
                    SUM(mhfm.konfirmasi_lab_rdt) as konfirmasi_lab_rdt,
                    SUM(mhfm.konfirmasi_lab_pcr) as konfirmasi_lab_pcr,
                    SUM(pos_0_4) as pos_0_4,
                    SUM(mhfm.pos_5_14) as pos_5_14,
                    SUM(mhfm.pos_15_64) as pos_15_64,
                    SUM(mhfm.pos_diatas_64) as pos_diatas_64,
                    SUM(mhfm.hamil_pos) as hamil_pos,
                    SUM(mhfm.kematian_malaria) as kematian_malaria,
                    SUM(mhfm.obat_standar) as obat_standar,
                    SUM(mhfm.obat_nonprogram) as obat_nonprogram,
                    SUM(mhfm.obat_primaquin) as obat_primaquin,
                    SUM(p_pf) as p_pf,
                    SUM(p_pv) as p_pv,
                    SUM(p_po) as p_po,
                    SUM(p_pm) as p_pm,
                    SUM(p_pk) as p_pk,
                    SUM(p_mix) as p_mix,
                    SUM(p_suspek_pk) as p_suspek_pk,
                    SUM(penularan_indigenus) as penularan_indigenus,
                    SUM(penularan_impor) as penularan_impor,
                    SUM(penularan_induced) as penularan_induced,
                    SUM(relaps) as relaps
                FROM 
                    malaria_health_facility_monthly mhfm
                JOIN 
                    health_facility_id hfi
                ON 
                    mhfm.id_faskes = hfi.id_faskes
                GROUP BY 
                    mhfm.tahun, mhfm.bulan, mhfm.status
            )
            UNION ALL
            (
                SELECT 
                    hfi.provinsi AS province,
                    mhfm.tahun AS year,
                    mhfm.bulan AS month,
                    mhfm.status AS status,
                    SUM(mhfm.tot_pos) AS tot_pos,
                    SUM(mhfm.konfirmasi_lab_mikroskop) as konfirmasi_lab_mikroskop,
                    SUM(mhfm.konfirmasi_lab_rdt) as konfirmasi_lab_rdt,
                    SUM(mhfm.konfirmasi_lab_pcr) as konfirmasi_lab_pcr,
                    SUM(pos_0_4) as pos_0_4,
                    SUM(mhfm.pos_5_14) as pos_5_14,
                    SUM(mhfm.pos_15_64) as pos_15_64,
                    SUM(mhfm.pos_diatas_64) as pos_diatas_64,
                    SUM(mhfm.hamil_pos) as hamil_pos,
                    SUM(mhfm.kematian_malaria) as kematian_malaria,
                    SUM(mhfm.obat_standar) as obat_standar,
                    SUM(mhfm.obat_nonprogram) as obat_nonprogram,
                    SUM(mhfm.obat_primaquin) as obat_primaquin,
                    SUM(p_pf) as p_pf,
                    SUM(p_pv) as p_pv,
                    SUM(p_po) as p_po,
                    SUM(p_pm) as p_pm,
                    SUM(p_pk) as p_pk,
                    SUM(p_mix) as p_mix,
                    SUM(p_suspek_pk) as p_suspek_pk,
                    SUM(penularan_indigenus) as penularan_indigenus,
                    SUM(penularan_impor) as penularan_impor,
                    SUM(penularan_induced) as penularan_induced,
                    SUM(relaps) as relaps
                FROM 
                    malaria_health_facility_monthly mhfm
                JOIN 
                    health_facility_id hfi
                ON 
                    mhfm.id_faskes = hfi.id_faskes
                GROUP BY 
                    hfi.provinsi, mhfm.tahun, mhfm.bulan, mhfm.status
            )
        )
        SELECT 
            *
        FROM 
            combined_data
        WHERE 
            province = :province
        """)

        # Tambahkan kondisi tambahan jika year atau month ada
        if year:
            query = text(str(query) + " AND year = :year")
        if month:
            query = text(str(query) + " AND month = :month")

        query = text(str(query) + " ORDER BY province, year, month;")

        # Eksekusi query menggunakan SQLAlchemy
        params = {'province': province}
        if year:
            params['year'] = year
        if month:
            params['month'] = month

        current_data = db.session.execute(query, params).fetchall()

        # Konversi ke dictionary untuk kemudahan manipulasi
        current_data = [row._asdict() for row in current_data]
        previous_data_index = {
            (row['province'], row['year'], row['month'], row['status']): row
            for row in current_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        for row in current_data:
            # Ambil data bulan sebelumnya
            prev_actual = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'actual'))
            prev_predicted = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'predicted'))
            prev_row = prev_actual if prev_actual else prev_predicted

            # Hitung perubahan M-to-M
            for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                        'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                        'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                        'p_pk', 'p_mix', 'p_suspek_pk', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps']:
                if prev_row and row[key] and prev_row[key]:
                    row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    row[f'{key}_m_to_m_change'] = None

            # Ambil data tahun sebelumnya di bulan yang sama
            yoy_actual = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'actual'))
            yoy_predicted = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'predicted'))
            yoy_row = yoy_actual if yoy_actual else yoy_predicted

            # Hitung perubahan Y-on-Y
            for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                        'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                        'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                        'p_pk', 'p_mix', 'p_suspek_pk', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps']:
                if yoy_row and row[key] and yoy_row[key]:
                    row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    row[f'{key}_y_on_y_change'] = None

            # Tambahkan hasil yang telah dihitung ke dalam final_data
            final_data.append(row)

        return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()