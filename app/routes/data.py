
from sqlalchemy import text
from flask import Blueprint, current_app, request, jsonify, send_file, send_from_directory
import pandas as pd
import os
from app import db
from app.models.db_models import MalariaMonthly,HealthFacility
from werkzeug.utils import secure_filename
import os
import tempfile
from flask_jwt_extended import jwt_required, get_jwt_identity
import requests


bp = Blueprint('data', __name__, url_prefix='')

# ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# def allowed_file(filename):
#     return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# @bp.route('/upload', methods=['POST'])
# @jwt_required()
# def upload_malaria_data():
#     # Cek user identity
#     current_user_id = get_jwt_identity()
    
#     # Validasi request body
#     if not request.is_json:
#         return jsonify({'error': 'Request harus berupa JSON'}), 400
    
#     request_data = request.get_json()
    
#     if 'data' not in request_data or not isinstance(request_data['data'], list) or not request_data['data']:
#         return jsonify({'error': 'Data tidak ditemukan atau format tidak valid'}), 400
    
#     excel_data = request_data['data']
#     try:
#         # Hasil proses
#         result = {
#             'health_facility_added': 0,
#             'health_facility_existed': 0,
#             'malaria_data_added': 0,
#             'malaria_data_updated': 0,
#             'errors': []
#         }
        
#         # Proses baris per baris dari data JSON
#         for index, row in enumerate(excel_data):
#             try:
#                 # Pastikan kolom yang diperlukan ada
#                 required_columns = ['id_faskes', 'bulan', 'tahun', 'provinsi', 'kabupaten', 'kecamatan']
#                 missing_columns = [col for col in required_columns if col not in row]
                
#                 if missing_columns:
#                     result['errors'].append(f'Baris ke-{index+1}: Kolom yang diperlukan tidak ditemukan: {missing_columns}')
#                     continue
                
#                 id_faskes = row['id_faskes']
#                 bulan = row['bulan']
#                 tahun = row['tahun']
                
#                 # Proses HealthFacilityId (pastikan tidak duplikat)
#                 existing_facility = HealthFacilityId.query.filter_by(
#                     id_faskes=id_faskes,
#                     provinsi=row['provinsi'],
#                     kabupaten=row['kabupaten'],
#                     kecamatan=row['kecamatan'],
#                     nama_faskes=row.get('nama_faskes')
#                 ).first()
                
#                 if not existing_facility:
#                     # Tambahkan fasilitas kesehatan baru
#                     new_facility = HealthFacilityId(
#                         id_faskes=id_faskes,
#                         provinsi=row['provinsi'],
#                         kabupaten=row['kabupaten'],
#                         kecamatan=row['kecamatan'],
#                         owner=row.get('owner'),
#                         tipe_faskes=row.get('tipe_faskes'),
#                         nama_faskes=row.get('nama_faskes'),
#                         address=row.get('address'),
#                         url=row.get('url'),
#                         lat=row.get('lat'),
#                         lon=row.get('lon')
#                     )
                    
#                     db.session.add(new_facility)
#                     db.session.flush()  # Flush untuk mendapatkan ID yang baru dibuat
#                     result['health_facility_added'] += 1
#                 else:
#                     result['health_facility_existed'] += 1
                
#                 # Proses MalariaHealthFacilityMonthly (update jika duplikat)
#                 existing_data = MalariaHealthFacilityMonthly.query.filter_by(
#                     id_faskes=id_faskes,
#                     bulan=bulan,
#                     tahun=tahun
#                 ).first()
                
#                 # Siapkan data malaria
#                 malaria_data = {
#                     'id_faskes': id_faskes,
#                     'bulan': bulan,
#                     'tahun': tahun,
#                     'status': 'actual',  # Pastikan status selalu "actual"
#                 }
                
#                 # Tambahkan kolom lain jika tersedia di data JSON
#                 # Add validation for data types
#                 for column, value in row.items():
#                     if column not in ['id_faskes', 'bulan', 'tahun', 'provinsi', 'kabupaten', 'kecamatan',
#                                     'owner', 'tipe_faskes', 'nama_faskes', 'address', 'url', 'lat', 'lon']:
#                         # Pastikan kolom ada di model MalariaHealthFacilityMonthly
#                         if hasattr(MalariaHealthFacilityMonthly, column) and value is not None:
#                             # Add type checking here
#                             column_type = MalariaHealthFacilityMonthly.__table__.columns[column].type.python_type
#                             try:
#                                 # Cast value to appropriate type
#                                 if column_type == float:
#                                     malaria_data[column] = float(value)
#                                 elif column_type == int:
#                                     malaria_data[column] = int(value)
#                                 elif column_type == str:
#                                     malaria_data[column] = str(value)
#                                 else:
#                                     malaria_data[column] = value
#                             except (ValueError, TypeError):
#                                 # Handle type conversion errors
#                                 error_msg = f"Error konversi tipe data untuk kolom {column}: nilai '{value}' tidak valid untuk tipe {column_type.__name__}"
#                                 result['errors'].append(error_msg)
#                                 continue
                
#                 if existing_data:
#                     # Update data yang sudah ada
#                     for key, value in malaria_data.items():
#                         setattr(existing_data, key, value)
#                     result['malaria_data_updated'] += 1
#                 else:
#                     # Tambahkan data baru
#                     new_malaria_data = MalariaHealthFacilityMonthly(**malaria_data)
#                     db.session.add(new_malaria_data)
#                     result['malaria_data_added'] += 1
                
#             except Exception as e:
#                 error_msg = f"Error pada baris ke-{index+1}: {str(e)}"
#                 result['errors'].append(error_msg)
        
#         # Commit semua perubahan ke database
#         db.session.commit()
        
#         return jsonify({
#             'message': 'Data berhasil diproses',
#             'result': result
#         }), 200
        
#     except Exception as e:
#         # Rollback jika terjadi error
#         db.session.rollback()
#         return jsonify({'error': f'Terjadi kesalahan: {str(e)}'}), 500
#     finally:
#         db.session.close()

@bp.route('/get-weather', methods=['GET'])
def get_weather():
    try:
        # Get parameters from the request
        params = request.args.get('params', '')
        # Construct the URL for the BMKG API with the correct parameter format
        bmkg_url = f'https://api.bmkg.go.id/publik/prakiraan-cuaca?{params}'
        
        # Add any required headers
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'ClimateSmartIndonesia/1.0',
            # Add any other headers the API might require
        }
        
        # Make the request to the BMKG API
        response = requests.get(bmkg_url, headers=headers, timeout=10)
        
        # Check if the request was successful
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({
                'success': False,
                'error': "Terjadi kesalahan, silahkan coba lagi"
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': "Terjadi kesalahan, silahkan coba lagi"
        }), 500

@bp.route('/get-provinces', methods=['GET'])
@jwt_required()
def get_provinces():
    try:
        # SQL query untuk mengambil data prediksi
        query = text("""
        SELECT kd_prov, provinsi as province, kd_bmkg as kd_bmkg_prov
        FROM 
            masterprov
        GROUP BY
            kd_prov, provinsi
        ORDER BY
            provinsi
        """)
        
        result = db.session.execute(query).fetchall()
        provinces = [{"code": row.kd_prov, "name": row.province,"bmkg":row.kd_bmkg_prov} for row in result]
        
        return jsonify({"data": provinces, "success": True}), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-cities', methods=['GET'])
@jwt_required()
def get_cities():
    try:
        province = request.args.get('province', default=None, type=str)
        if province == '00':
            province = None
        
        # SQL query untuk mengambil data prediksi
        query = text("""
        SELECT kd_prov, kd_kab,kabkot as city,kd_bmkg as kd_bmkg_kab, status_endemis
        FROM 
            masterkab
        WHERE kd_prov = :province
        """)
        params = {}
        params['province'] = province
        # Eksekusi query
        result = db.session.execute(query, params).fetchall()
        
        cities = [{"code": row.kd_kab, "name": row.city,"bmkg":row.kd_bmkg_kab,"status_endemis":row.status_endemis} for row in result]
        
        return jsonify({"data": cities, "success": True}), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-districts', methods=['GET'])
@jwt_required()
def get_districts():
    try:
        city = request.args.get('city', default=None, type=str)
        if city == '':
            city = None
        # SQL query untuk mengambil data prediksi
        query = text("""
        SELECT kd_kab,kd_kec,kecamatan as district,kd_bmkg as kd_bmkg_kec
        FROM 
            masterkec
        WHERE kd_kab = :city
        """)
        params = {}
        params['city'] = city
        # Eksekusi query
        result = db.session.execute(query, params).fetchall()
        
        districts = [{"code": row.kd_kec, "name": row.district,"bmkg":row.kd_bmkg_kec} for row in result]
        
        return jsonify({"data": districts, "success": True}), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-warning-malaria', methods=['GET'])
@jwt_required()
def get_warning_malaria():
    try:
        query = text("""
        SELECT *
        FROM
        (SELECT hfi.kd_kab,hfi.kabkot,hfi.status_endemis,
        hfi.kd_prov,hfi.provinsi,bulan as month,tahun as year,status,sum(tot_pos) as predicted_tot_pos, sum(kematian_malaria) as predicted_kematian_malaria, sum(penularan_indigenus) as predicted_penularan_indigenus
        FROM malariamonthly
        JOIN 
                                (
                                select hf.*,mp.provinsi,mk.kabkot,mk.status_endemis
                                from healthfacility hf
                                left join masterkab mk
                                on hf.kd_kab = mk.kd_kab
                                left join masterprov mp 
                                on hf.kd_prov = mp.kd_prov) hfi
        ON malariamonthly.id_faskes = hfi.id_faskes
        GROUP BY hfi.kd_kab,hfi.kabkot,hfi.kd_prov,hfi.provinsi,status,bulan,tahun,status_endemis)
        """)
        # Eksekusi query
        result = db.session.execute(query).fetchall()
        # Format hasil query sesuai dengan data yang diambil
        malaria_data = []
        for row in result:
            malaria_data.append({
                "kd_kab": row.kd_kab,
                "city": row.kabkot,
                "status_endemis": row.status_endemis,
                "kd_prov": row.kd_prov,
                "province": row.provinsi,
                "month": row.month,
                "year": row.year,
                "predicted_tot_pos": row.predicted_tot_pos,
                "predicted_kematian_malaria": row.predicted_kematian_malaria,
                "predicted_penularan_indigenus": row.predicted_penularan_indigenus,
                "status":row.status
            })
        
        previous_data_index = {
                    (row['province'],row['city'], row['year'], row['month'], row['status']): row
                    for row in malaria_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        for row in malaria_data:
            # Ambil data bulan sebelumnya
            
            if row['month'] == 1:  # January - look at previous year's December
                prev_actual = previous_data_index.get((row['province'],row['city'], row['year'] - 1, 12, 'actual'))
                prev_predicted = previous_data_index.get((row['province'],row['city'], row['year'] - 1, 12, 'predicted'))
            else:
                prev_actual = previous_data_index.get((row['province'],row['city'], row['year'], row['month'] - 1, 'actual'))
                prev_predicted = previous_data_index.get((row['province'],row['city'], row['year'], row['month'] - 1, 'predicted'))

            prev_row = prev_actual if prev_actual else prev_predicted

            # Hitung perubahan M-to-M
            for key in ['predicted_penularan_indigenus']:
                if prev_row and row[key] and prev_row[key]:
                    row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    row[f'{key}_m_to_m_change'] = None     
                final_data.append(row)   
        
        return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()

# @bp.route('/get-facilities', methods=['GET'])
# @jwt_required()
# def get_facilities():
#     """Get list of facilities for prediction"""
#     try:
#         province = request.args.get('province', default=None, type=str)
#         kabupaten = request.args.get('kabupaten', default=None, type=str)
        
#         query = HealthFacility.query
        
#         if province and province != 'TOTAL':
#             query = query.filter_by(provinsi=province)
#         if kabupaten:
#             query = query.filter_by(kabupaten=kabupaten)
            
#         facilities = query.all()
        
#         return jsonify({
#             "success": True,
#             "facilities": [{
#                 "id_faskes": f.id_faskes,
#                 "nama_faskes": f.nama_faskes,
#                 "tipe_faskes": f.tipe_faskes,
#                 "provinsi": f.provinsi,
#                 "kabupaten": f.kabupaten,
#                 "kecamatan": f.kecamatan
#             } for f in facilities]
#         }), 200
        
#     except Exception as e:
#         return jsonify({
#             "success": False,
#             "error": str(e)
#         }), 500
#     finally:
#         db.session.close()

@bp.route('/get-raw-data-malaria', methods=['GET'])
@jwt_required()
def get_raw_data_malaria():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        district = request.args.get('district', default=None, type=str)
        
        if province == '00':
            province = None
        
        # Ambil parameter month_year jika ada
        month_year = request.args.get('month_year', default=None, type=str)
        month = None
        year = None
        
        if month_year:
            # Format yang diharapkan: "YYYY-MM"
            try:
                year, month = map(int, month_year.split('-'))
            except (ValueError, IndexError):
                return jsonify({"error": "Format month_year harus YYYY-MM"}), 400
        
        # SQL query untuk periode saat ini
        current_query = text("""
            SELECT 
                hfi.kd_prov,hfi.kd_kab,hfi.kd_kec,
                    hfi.provinsi AS province,
                    hfi.kabkot AS city,
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
                malariamonthly mhfm
            JOIN 
                (   select hf.*,mp.provinsi,mk.kabkot,mkec.kecamatan
                    from healthfacility hf
                    left join masterkec mkec
                    on hf.kd_kec = mkec.kd_kec
                    left join masterkab mk
                    on hf.kd_kab = mk.kd_kab
                    left join masterprov mp 
                    on hf.kd_prov = mp.kd_prov) hfi
            ON 
                mhfm.id_faskes = hfi.id_faskes
        """)
        
        # Build the WHERE clause
        where_clauses = []
        params = {}
        
        if province:
            where_clauses.append("hfi.kd_prov = :province")
            params['province'] = province
            
        if city:
            where_clauses.append("hfi.kd_kab = :city")
            params['city'] = city
            
        if district:
            where_clauses.append("hfi.kd_kec = :district")
            params['district'] = district
            
        # Add WHERE clause to the query if any conditions exist
        if where_clauses:
            current_query = text(str(current_query) + " WHERE " + " AND ".join(where_clauses))
        
        # Eksekusi query
        current_data = db.session.execute(current_query, params).fetchall()
        
        # Konversi ke dictionary untuk kemudahan manipulasi
        current_data = [row._asdict() for row in current_data]
        previous_data_index = {
            (row['id_faskes'], row['year'], row['month']): row
            for row in current_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        for row in current_data:
            # Check if current month is January to properly handle month-to-month change
            if row['month'] == 1:
                # For January, look at December of previous year
                prev_row = previous_data_index.get((row['id_faskes'], row['year'] - 1, 12))
            else:
                # For all other months, look at previous month in same year
                prev_row = previous_data_index.get((row['id_faskes'], row['year'], row['month'] - 1))
            
            for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                                'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                                'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                                'p_pk', 'p_mix', 'p_suspek_pk', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps']:
                if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                    row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    row[f'{key}_m_to_m_change'] = None

                yoy_row = previous_data_index.get((row['id_faskes'], row['year'] - 1, row['month']))
                if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                    row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    row[f'{key}_y_on_y_change'] = None

            # Tambahkan hasil yang telah dihitung ke dalam final_data
            final_data.append(row)
        
        # Langkah 1: Cari tahun dan bulan terakhir yang status="actual"
        actual_data = [row for row in final_data if row['status'] == 'actual']
        if not actual_data:
            return jsonify({"error": "No actual data found"}), 404
            
        max_year_actual = max(row['year'] for row in actual_data)
        max_month_actual = max(row['month'] for row in actual_data if row['year'] == max_year_actual)
        
        # Tentukan bulan prediksi pertama setelah actual terakhir
        if max_month_actual == 12:
            next_month_predicted = 1
            next_year_predicted = max_year_actual + 1
        else:
            next_month_predicted = max_month_actual + 1
            next_year_predicted = max_year_actual
        
        # Jika month_year tidak disediakan, default ke bulan dan tahun pertama dengan data predicted
        if month is None or year is None:
            month = next_month_predicted
            year = next_year_predicted
        
        # Filter data berdasarkan bulan dan tahun yang dipilih
        # Status tidak difilter di parameter, akan ditentukan dari data yang ada
        filtered_data = [
            row for row in final_data 
            if row['year'] == year and row['month'] == month
        ]
        
        return jsonify({
            "data": filtered_data, 
            "metadata": {
                "current_filter": {
                    "year": year,
                    "month": month,
                    "status": filtered_data[0]['status'] if filtered_data else None
                },
                "last_actual": {
                    "year": max_year_actual,
                    "month": max_month_actual
                },
                "first_predicted": {
                    "year": next_year_predicted,
                    "month": next_month_predicted
                }
            },
            "success": True
        }), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-aggregate-data-malaria', methods=['GET'])
@jwt_required()
def get_aggregate_data_malaria():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        district = request.args.get('district', default=None, type=str)
        start_month_year = request.args.get('start', default=None, type=str)
        end_month_year = request.args.get('end', default=None, type=str)
        
        
        if not province:
            return jsonify({"error": "Parameter 'province' is required"}), 400
            
        # Parse start and end dates if provided
        date_range_condition = ""
        if start_month_year and end_month_year:
            # Parse start and end dates dari format "2021-1"
            start_parts = start_month_year.split('-')
            start_year = int(start_parts[0])
            start_month = int(start_parts[1])

            end_parts = end_month_year.split('-')
            end_year = int(end_parts[0])
            end_month = int(end_parts[1])

            # Calculate date values untuk perbandingan (format: YYYYMM)
            start_date_value = start_year * 100 + start_month
            end_date_value = end_year * 100 + end_month
            date_range_condition = " AND (year * 100 + month) >= :start_date_value AND (year * 100 + month) <= :end_date_value"
        
        if city:
            if district:
                query = text("""
                WITH combined_data AS (
                    SELECT 
                        hfi.kd_prov, hfi.kd_kab, hfi.kd_kec,
                        hfi.provinsi AS province,
                        hfi.kabkot AS city,
                        hfi.kecamatan AS district,
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
                        SUM(p_others) as p_others,
                        SUM(penularan_indigenus) as penularan_indigenus,
                        SUM(penularan_impor) as penularan_impor,
                        SUM(penularan_induced) as penularan_induced,
                        SUM(relaps) as relaps
                    FROM 
                        malariamonthly mhfm
                    JOIN 
                        (
                        select hf.*,mp.provinsi,mk.kabkot,mkec.kecamatan
                        from healthfacility hf
                        left join masterkec mkec
                        on hf.kd_kec = mkec.kd_kec
                        left join masterkab mk
                        on hf.kd_kab = mk.kd_kab
                        left join masterprov mp 
                        on hf.kd_prov = mp.kd_prov) hfi
                    ON 
                        mhfm.id_faskes = hfi.id_faskes
                    GROUP BY 
                        hfi.kd_prov,hfi.kd_kab,hfi.kd_kec,hfi.provinsi,hfi.kabkot,hfi.kecamatan, mhfm.tahun, mhfm.bulan, mhfm.status
                )
                SELECT 
                    combined_data.*,
                    AVG(climatemonthly.hujan_hujan_mean) as hujan_mean,
                    AVG(climatemonthly.hujan_hujan_max) as hujan_max,
                    AVG(climatemonthly.hujan_hujan_min) as hujan_min,
                    AVG(climatemonthly.rh_rh_mean) as rh_mean,
                    AVG(climatemonthly.rh_rh_max) as rh_max,
                    AVG(climatemonthly.rh_rh_min) as rh_min,
                    AVG(climatemonthly.tm_tm_mean) as tm_mean,
                    AVG(climatemonthly.tm_tm_max) as tm_max,
                    AVG(climatemonthly.tm_tm_min) as tm_min,
                    AVG(climatemonthly.max_value_10u) as max_value_10u,
                    AVG(climatemonthly.max_value_10v) as max_value_10v,
                    AVG(climatemonthly.max_value_2d) as max_value_2d,
                    AVG(climatemonthly.max_value_2t) as max_value_2t,
                    AVG(climatemonthly.max_value_cp) as max_value_cp,
                    AVG(climatemonthly.max_value_crr) as max_value_crr,
                    AVG(climatemonthly.max_value_cvh) as max_value_cvh,
                    AVG(climatemonthly.max_value_cvl) as max_value_cvl,
                    AVG(climatemonthly.max_value_e) as max_value_e,
                    AVG(climatemonthly.max_value_lmlt) as max_value_lmlt,
                    AVG(climatemonthly.max_value_msl) as max_value_msl,
                    AVG(climatemonthly.max_value_ro) as max_value_ro,
                    AVG(climatemonthly.max_value_skt) as max_value_skt,
                    AVG(climatemonthly.max_value_sp) as max_value_sp,
                    AVG(climatemonthly.max_value_sro) as max_value_sro,
                    AVG(climatemonthly.max_value_swvl1) as max_value_swvl1,
                    AVG(climatemonthly.max_value_tcc) as max_value_tcc,
                    AVG(climatemonthly.max_value_tcrw) as max_value_tcrw,
                    AVG(climatemonthly.max_value_tcwv) as max_value_tcwv,
                    AVG(climatemonthly.max_value_tp) as max_value_tp,
                    AVG(climatemonthly.mean_value_10u) as mean_value_10u,
                    AVG(climatemonthly.mean_value_10v) as mean_value_10v,
                    AVG(climatemonthly.mean_value_2d) as mean_value_2d,
                    AVG(climatemonthly.mean_value_2t) as mean_value_2t,
                    AVG(climatemonthly.mean_value_cp) as mean_value_cp,
                    AVG(climatemonthly.mean_value_crr) as mean_value_crr,
                    AVG(climatemonthly.mean_value_cvh) as mean_value_cvh,
                    AVG(climatemonthly.mean_value_cvl) as mean_value_cvl,
                    AVG(climatemonthly.mean_value_e) as mean_value_e,
                    AVG(climatemonthly.mean_value_lmlt) as mean_value_lmlt,
                    AVG(climatemonthly.mean_value_msl) as mean_value_msl,
                    AVG(climatemonthly.mean_value_ro) as mean_value_ro,
                    AVG(climatemonthly.mean_value_skt) as mean_value_skt,
                    AVG(climatemonthly.mean_value_sp) as mean_value_sp,
                    AVG(climatemonthly.mean_value_sro) as mean_value_sro,
                    AVG(climatemonthly.mean_value_swvl1) as mean_value_swvl1,
                    AVG(climatemonthly.mean_value_tcc) as mean_value_tcc,
                    AVG(climatemonthly.mean_value_tcrw) as mean_value_tcrw,
                    AVG(climatemonthly.mean_value_tcwv) as mean_value_tcwv,
                    AVG(climatemonthly.mean_value_tp) as mean_value_tp,
                    AVG(climatemonthly.min_value_10u) as min_value_10u,
                    AVG(climatemonthly.min_value_10v) as min_value_10v,
                    AVG(climatemonthly.min_value_2d) as min_value_2d,
                    AVG(climatemonthly.min_value_2t) as min_value_2t,
                    AVG(climatemonthly.min_value_cp) as min_value_cp,
                    AVG(climatemonthly.min_value_crr) as min_value_crr,
                    AVG(climatemonthly.min_value_cvh) as min_value_cvh,
                    AVG(climatemonthly.min_value_cvl) as min_value_cvl,
                    AVG(climatemonthly.min_value_e) as min_value_e,
                    AVG(climatemonthly.min_value_lmlt) as min_value_lmlt,
                    AVG(climatemonthly.min_value_msl) as min_value_msl,
                    AVG(climatemonthly.min_value_ro) as min_value_ro,
                    AVG(climatemonthly.min_value_skt) as min_value_skt,
                    AVG(climatemonthly.min_value_sp) as min_value_sp,
                    AVG(climatemonthly.min_value_sro) as min_value_sro,
                    AVG(climatemonthly.min_value_swvl1) as min_value_swvl1,
                    AVG(climatemonthly.min_value_tcc) as min_value_tcc,
                    AVG(climatemonthly.min_value_tcrw) as min_value_tcrw,
                    AVG(climatemonthly.min_value_tcwv) as min_value_tcwv,
                    AVG(climatemonthly.min_value_tp) as min_value_tp
                FROM 
                    combined_data
                LEFT JOIN
                    climatemonthly
                ON 
                    combined_data.kd_kec = climatemonthly.kd_kec
                    AND combined_data.year = climatemonthly.tahun
                    AND combined_data.month = climatemonthly.bulan
                WHERE 
                    combined_data.kd_prov = :province
                    AND combined_data.kd_kab = :city
                    AND combined_data.kd_kec = :district
                GROUP BY
                    combined_data.kd_prov, combined_data.kd_kab, combined_data.kd_kec, 
                    combined_data.province, combined_data.city, combined_data.district,
                    combined_data.year, combined_data.month, combined_data.status,
                    combined_data.tot_pos, combined_data.konfirmasi_lab_mikroskop, 
                    combined_data.konfirmasi_lab_rdt, combined_data.konfirmasi_lab_pcr,
                    combined_data.pos_0_4, combined_data.pos_5_14, combined_data.pos_15_64,
                    combined_data.pos_diatas_64, combined_data.hamil_pos, combined_data.kematian_malaria,
                    combined_data.obat_standar, combined_data.obat_nonprogram, combined_data.obat_primaquin,
                    combined_data.p_pf, combined_data.p_pv, combined_data.p_po, combined_data.p_pm,
                    combined_data.p_pk, combined_data.p_mix, combined_data.p_suspek_pk, combined_data.p_others,
                    combined_data.penularan_indigenus, combined_data.penularan_impor, combined_data.penularan_induced,
                    combined_data.relaps
                    """)

                # Tambahkan kondisi tambahan
                if start_month_year and end_month_year:
                    query = text(str(query) + date_range_condition)

                query = text(str(query) + " ORDER BY province,city,district, year, month;")

                # Eksekusi query menggunakan SQLAlchemy
                params = {'province': province, 
                          'city': city, 
                          'district': district}
                if start_month_year and end_month_year:
                    params['start_date_value'] = start_date_value
                    params['end_date_value'] = end_date_value

                current_data = db.session.execute(query, params).fetchall()

                # Konversi ke dictionary untuk kemudahan manipulasi
                current_data = [row._asdict() for row in current_data]
                previous_data_index = {
                    (row['province'],row['city'],row['district'], row['year'], row['month'], row['status']): row
                    for row in current_data
                }

                # Hitung perubahan M-to-M dan Y-on-Y
                final_data = []
                for row in current_data:
                    # Ambil data bulan sebelumnya
                    if row['month'] == 1:  # January - look at previous year's December
                        prev_actual = previous_data_index.get((row['province'],row['city'],row['district'], row['year'] - 1, 12, 'actual'))
                        prev_predicted = previous_data_index.get((row['province'],row['city'],row['district'], row['year'] - 1, 12, 'predicted'))
                    else:
                        prev_actual = previous_data_index.get((row['province'],row['city'],row['district'], row['year'], row['month'] - 1, 'actual'))
                        prev_predicted = previous_data_index.get((row['province'],row['city'],row['district'], row['year'], row['month'] - 1, 'predicted'))

                    prev_row = prev_actual if prev_actual else prev_predicted

                    # Hitung perubahan M-to-M
                    for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                                'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                                'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                                'p_pk', 'p_mix', 'p_suspek_pk', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps','hujan_mean', 'hujan_max',
                                'hujan_min', 'rh_mean', 'rh_max', 'rh_min', 'tm_mean',
                                'tm_max', 'tm_min', 'max_value_10u', 'max_value_10v',
                                'max_value_2d', 'max_value_2t', 'max_value_cp', 'max_value_crr',
                                'max_value_cvh', 'max_value_cvl', 'max_value_e', 'max_value_lmlt',
                                'max_value_msl', 'max_value_ro', 'max_value_skt', 'max_value_sp',
                                'max_value_sro', 'max_value_swvl1', 'max_value_tcc', 'max_value_tcrw',
                                'max_value_tcwv', 'max_value_tp', 'mean_value_10u', 'mean_value_10v',
                                'mean_value_2d', 'mean_value_2t', 'mean_value_cp', 'mean_value_crr',
                                'mean_value_cvh', 'mean_value_cvl', 'mean_value_e', 'mean_value_lmlt',
                                'mean_value_msl', 'mean_value_ro', 'mean_value_skt', 'mean_value_sp',
                                'mean_value_sro', 'mean_value_swvl1', 'mean_value_tcc',
                                'mean_value_tcrw', 'mean_value_tcwv', 'mean_value_tp', 'min_value_10u',
                                'min_value_10v', 'min_value_2d', 'min_value_2t', 'min_value_cp',
                                'min_value_crr', 'min_value_cvh', 'min_value_cvl', 'min_value_e',
                                'min_value_lmlt', 'min_value_msl', 'min_value_ro', 'min_value_skt',
                                'min_value_sp', 'min_value_sro', 'min_value_swvl1', 'min_value_tcc',
                                'min_value_tcrw', 'min_value_tcwv', 'min_value_tp']:
                        if prev_row and row[key] and prev_row[key]:
                            row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                        else:
                            row[f'{key}_m_to_m_change'] = None

                    # Ambil data tahun sebelumnya di bulan yang sama
                    yoy_actual = previous_data_index.get((row['province'],row['city'],row['district'], row['year'] - 1, row['month'], 'actual'))
                    yoy_predicted = previous_data_index.get((row['province'],row['city'],row['district'], row['year'] - 1, row['month'], 'predicted'))
                    yoy_row = yoy_actual if yoy_actual else yoy_predicted

                    # Hitung perubahan Y-on-Y
                    for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                                'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                                'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                                'p_pk', 'p_mix', 'p_suspek_pk', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps','hujan_mean', 'hujan_max',
                                'hujan_min', 'rh_mean', 'rh_max', 'rh_min', 'tm_mean',
                                'tm_max', 'tm_min', 'max_value_10u', 'max_value_10v',
                                'max_value_2d', 'max_value_2t', 'max_value_cp', 'max_value_crr',
                                'max_value_cvh', 'max_value_cvl', 'max_value_e', 'max_value_lmlt',
                                'max_value_msl', 'max_value_ro', 'max_value_skt', 'max_value_sp',
                                'max_value_sro', 'max_value_swvl1', 'max_value_tcc', 'max_value_tcrw',
                                'max_value_tcwv', 'max_value_tp', 'mean_value_10u', 'mean_value_10v',
                                'mean_value_2d', 'mean_value_2t', 'mean_value_cp', 'mean_value_crr',
                                'mean_value_cvh', 'mean_value_cvl', 'mean_value_e', 'mean_value_lmlt',
                                'mean_value_msl', 'mean_value_ro', 'mean_value_skt', 'mean_value_sp',
                                'mean_value_sro', 'mean_value_swvl1', 'mean_value_tcc',
                                'mean_value_tcrw', 'mean_value_tcwv', 'mean_value_tp', 'min_value_10u',
                                'min_value_10v', 'min_value_2d', 'min_value_2t', 'min_value_cp',
                                'min_value_crr', 'min_value_cvh', 'min_value_cvl', 'min_value_e',
                                'min_value_lmlt', 'min_value_msl', 'min_value_ro', 'min_value_skt',
                                'min_value_sp', 'min_value_sro', 'min_value_swvl1', 'min_value_tcc',
                                'min_value_tcrw', 'min_value_tcwv', 'min_value_tp']:
                        if yoy_row and row[key] and yoy_row[key]:
                            row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                        else:
                            row[f'{key}_y_on_y_change'] = None

                    # Tambahkan hasil yang telah dihitung ke dalam final_data
                    final_data.append(row)

                return jsonify({"data": final_data, "success": True}), 200
            else:
                query = text("""
                WITH combined_data AS (
                    SELECT 
                        hfi.kd_prov, hfi.kd_kab,
                        hfi.provinsi AS province,
                        hfi.kabkot AS city,
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
                        SUM(p_others) as p_others,
                        SUM(penularan_indigenus) as penularan_indigenus,
                        SUM(penularan_impor) as penularan_impor,
                        SUM(penularan_induced) as penularan_induced,
                        SUM(relaps) as relaps
                    FROM 
                        malariamonthly mhfm
                    JOIN 
                        (
                        select hf.*,mp.provinsi,mk.kabkot
                        from healthfacility hf
                        left join masterkab mk
                        on hf.kd_kab = mk.kd_kab
                        left join masterprov mp 
                        on hf.kd_prov = mp.kd_prov) hfi
                    ON 
                        mhfm.id_faskes = hfi.id_faskes
                    GROUP BY 
                        hfi.kd_prov,hfi.kd_kab,hfi.provinsi,hfi.kabkot, mhfm.tahun, mhfm.bulan, mhfm.status
                ),
                aggregated_climate AS (
                    SELECT
                        mkec.kd_prov,
                        mkec.kd_kab,
                        climatemonthly.tahun,
                        climatemonthly.bulan,
                        AVG(climatemonthly.hujan_hujan_mean) as hujan_mean,
                        AVG(climatemonthly.hujan_hujan_max) as hujan_max,
                        AVG(climatemonthly.hujan_hujan_min) as hujan_min,
                        AVG(climatemonthly.rh_rh_mean) as rh_mean,
                        AVG(climatemonthly.rh_rh_max) as rh_max,
                        AVG(climatemonthly.rh_rh_min) as rh_min,
                        AVG(climatemonthly.tm_tm_mean) as tm_mean,
                        AVG(climatemonthly.tm_tm_max) as tm_max,
                        AVG(climatemonthly.tm_tm_min) as tm_min,
                        AVG(climatemonthly.max_value_10u) as max_value_10u,
                        AVG(climatemonthly.max_value_10v) as max_value_10v,
                        AVG(climatemonthly.max_value_2d) as max_value_2d,
                        AVG(climatemonthly.max_value_2t) as max_value_2t,
                        AVG(climatemonthly.max_value_cp) as max_value_cp,
                        AVG(climatemonthly.max_value_crr) as max_value_crr,
                        AVG(climatemonthly.max_value_cvh) as max_value_cvh,
                        AVG(climatemonthly.max_value_cvl) as max_value_cvl,
                        AVG(climatemonthly.max_value_e) as max_value_e,
                        AVG(climatemonthly.max_value_lmlt) as max_value_lmlt,
                        AVG(climatemonthly.max_value_msl) as max_value_msl,
                        AVG(climatemonthly.max_value_ro) as max_value_ro,
                        AVG(climatemonthly.max_value_skt) as max_value_skt,
                        AVG(climatemonthly.max_value_sp) as max_value_sp,
                        AVG(climatemonthly.max_value_sro) as max_value_sro,
                        AVG(climatemonthly.max_value_swvl1) as max_value_swvl1,
                        AVG(climatemonthly.max_value_tcc) as max_value_tcc,
                        AVG(climatemonthly.max_value_tcrw) as max_value_tcrw,
                        AVG(climatemonthly.max_value_tcwv) as max_value_tcwv,
                        AVG(climatemonthly.max_value_tp) as max_value_tp,
                        AVG(climatemonthly.mean_value_10u) as mean_value_10u,
                        AVG(climatemonthly.mean_value_10v) as mean_value_10v,
                        AVG(climatemonthly.mean_value_2d) as mean_value_2d,
                        AVG(climatemonthly.mean_value_2t) as mean_value_2t,
                        AVG(climatemonthly.mean_value_cp) as mean_value_cp,
                        AVG(climatemonthly.mean_value_crr) as mean_value_crr,
                        AVG(climatemonthly.mean_value_cvh) as mean_value_cvh,
                        AVG(climatemonthly.mean_value_cvl) as mean_value_cvl,
                        AVG(climatemonthly.mean_value_e) as mean_value_e,
                        AVG(climatemonthly.mean_value_lmlt) as mean_value_lmlt,
                        AVG(climatemonthly.mean_value_msl) as mean_value_msl,
                        AVG(climatemonthly.mean_value_ro) as mean_value_ro,
                        AVG(climatemonthly.mean_value_skt) as mean_value_skt,
                        AVG(climatemonthly.mean_value_sp) as mean_value_sp,
                        AVG(climatemonthly.mean_value_sro) as mean_value_sro,
                        AVG(climatemonthly.mean_value_swvl1) as mean_value_swvl1,
                        AVG(climatemonthly.mean_value_tcc) as mean_value_tcc,
                        AVG(climatemonthly.mean_value_tcrw) as mean_value_tcrw,
                        AVG(climatemonthly.mean_value_tcwv) as mean_value_tcwv,
                        AVG(climatemonthly.mean_value_tp) as mean_value_tp,
                        AVG(climatemonthly.min_value_10u) as min_value_10u,
                        AVG(climatemonthly.min_value_10v) as min_value_10v,
                        AVG(climatemonthly.min_value_2d) as min_value_2d,
                        AVG(climatemonthly.min_value_2t) as min_value_2t,
                        AVG(climatemonthly.min_value_cp) as min_value_cp,
                        AVG(climatemonthly.min_value_crr) as min_value_crr,
                        AVG(climatemonthly.min_value_cvh) as min_value_cvh,
                        AVG(climatemonthly.min_value_cvl) as min_value_cvl,
                        AVG(climatemonthly.min_value_e) as min_value_e,
                        AVG(climatemonthly.min_value_lmlt) as min_value_lmlt,
                        AVG(climatemonthly.min_value_msl) as min_value_msl,
                        AVG(climatemonthly.min_value_ro) as min_value_ro,
                        AVG(climatemonthly.min_value_skt) as min_value_skt,
                        AVG(climatemonthly.min_value_sp) as min_value_sp,
                        AVG(climatemonthly.min_value_sro) as min_value_sro,
                        AVG(climatemonthly.min_value_swvl1) as min_value_swvl1,
                        AVG(climatemonthly.min_value_tcc) as min_value_tcc,
                        AVG(climatemonthly.min_value_tcrw) as min_value_tcrw,
                        AVG(climatemonthly.min_value_tcwv) as min_value_tcwv,
                        AVG(climatemonthly.min_value_tp) as min_value_tp
                    FROM 
                        climatemonthly
                    JOIN
                        masterkec mkec
                    ON 
                        climatemonthly.kd_kec = mkec.kd_kec
                    GROUP BY
                        mkec.kd_prov, mkec.kd_kab, climatemonthly.tahun, climatemonthly.bulan
                )
                SELECT 
                    cd.*,
                    ac.hujan_mean,
                    ac.hujan_max,
                    ac.hujan_min,
                    ac.rh_mean,
                    ac.rh_max,
                    ac.rh_min,
                    ac.tm_mean,
                    ac.tm_max,
                    ac.tm_min,
                    ac.max_value_10u,
                    ac.max_value_10v,
                    ac.max_value_2d,
                    ac.max_value_2t,
                    ac.max_value_cp,
                    ac.max_value_crr,
                    ac.max_value_cvh,
                    ac.max_value_cvl,
                    ac.max_value_e,
                    ac.max_value_lmlt,
                    ac.max_value_msl,
                    ac.max_value_ro,
                    ac.max_value_skt,
                    ac.max_value_sp,
                    ac.max_value_sro,
                    ac.max_value_swvl1,
                    ac.max_value_tcc,
                    ac.max_value_tcrw,
                    ac.max_value_tcwv,
                    ac.max_value_tp,
                    ac.mean_value_10u,
                    ac.mean_value_10v,
                    ac.mean_value_2d,
                    ac.mean_value_2t,
                    ac.mean_value_cp,
                    ac.mean_value_crr,
                    ac.mean_value_cvh,
                    ac.mean_value_cvl,
                    ac.mean_value_e,
                    ac.mean_value_lmlt,
                    ac.mean_value_msl,
                    ac.mean_value_ro,
                    ac.mean_value_skt,
                    ac.mean_value_sp,
                    ac.mean_value_sro,
                    ac.mean_value_swvl1,
                    ac.mean_value_tcc,
                    ac.mean_value_tcrw,
                    ac.mean_value_tcwv,
                    ac.mean_value_tp,
                    ac.min_value_10u,
                    ac.min_value_10v,
                    ac.min_value_2d,
                    ac.min_value_2t,
                    ac.min_value_cp,
                    ac.min_value_crr,
                    ac.min_value_cvh,
                    ac.min_value_cvl,
                    ac.min_value_e,
                    ac.min_value_lmlt,
                    ac.min_value_msl,
                    ac.min_value_ro,
                    ac.min_value_skt,
                    ac.min_value_sp,
                    ac.min_value_sro,
                    ac.min_value_swvl1,
                    ac.min_value_tcc,
                    ac.min_value_tcrw,
                    ac.min_value_tcwv,
                    ac.min_value_tp
                FROM 
                    combined_data cd
                LEFT JOIN
                    aggregated_climate ac
                ON 
                    cd.kd_prov = ac.kd_prov
                    AND cd.kd_kab = ac.kd_kab
                    AND cd.year = ac.tahun
                    AND cd.month = ac.bulan
                WHERE 
                    cd.kd_prov = :province 
                    AND cd.kd_kab = :city
                """)

                # Tambahkan kondisi untuk rentang tanggal
                if start_month_year and end_month_year:
                    query = text(str(query) + date_range_condition)

                query = text(str(query) + " ORDER BY province,city, year, month;")

                # Eksekusi query menggunakan SQLAlchemy
                params = {'province': province, 
                          'city': city}
                if start_month_year and end_month_year:
                    params['start_date_value'] = start_date_value
                    params['end_date_value'] = end_date_value

                current_data = db.session.execute(query, params).fetchall()

                # Konversi ke dictionary untuk kemudahan manipulasi
                current_data = [row._asdict() for row in current_data]
                previous_data_index = {
                    (row['province'],row['city'], row['year'], row['month'], row['status']): row
                    for row in current_data
                }

                # Hitung perubahan M-to-M dan Y-on-Y
                final_data = []
                for row in current_data:
                    # Ambil data bulan sebelumnya
                    if row['month'] == 1:  # January - look at previous year's December
                        prev_actual = previous_data_index.get((row['province'],row['city'], row['year'] - 1, 12, 'actual'))
                        prev_predicted = previous_data_index.get((row['province'],row['city'], row['year'] - 1, 12, 'predicted'))
                    else:
                        prev_actual = previous_data_index.get((row['province'],row['city'], row['year'], row['month'] - 1, 'actual'))
                        prev_predicted = previous_data_index.get((row['province'],row['city'], row['year'], row['month'] - 1, 'predicted'))

                    prev_row = prev_actual if prev_actual else prev_predicted

                    # Hitung perubahan M-to-M
                    for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                                'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                                'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                                'p_pk', 'p_mix', 'p_suspek_pk', 'p_others', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps','hujan_mean', 'hujan_max',
                                'hujan_min', 'rh_mean', 'rh_max', 'rh_min', 'tm_mean',
                                'tm_max', 'tm_min', 'max_value_10u', 'max_value_10v',
                                'max_value_2d', 'max_value_2t', 'max_value_cp', 'max_value_crr',
                                'max_value_cvh', 'max_value_cvl', 'max_value_e', 'max_value_lmlt',
                                'max_value_msl', 'max_value_ro', 'max_value_skt', 'max_value_sp',
                                'max_value_sro', 'max_value_swvl1', 'max_value_tcc', 'max_value_tcrw',
                                'max_value_tcwv', 'max_value_tp', 'mean_value_10u', 'mean_value_10v',
                                'mean_value_2d', 'mean_value_2t', 'mean_value_cp', 'mean_value_crr',
                                'mean_value_cvh', 'mean_value_cvl', 'mean_value_e', 'mean_value_lmlt',
                                'mean_value_msl', 'mean_value_ro', 'mean_value_skt', 'mean_value_sp',
                                'mean_value_sro', 'mean_value_swvl1', 'mean_value_tcc',
                                'mean_value_tcrw', 'mean_value_tcwv', 'mean_value_tp', 'min_value_10u',
                                'min_value_10v', 'min_value_2d', 'min_value_2t', 'min_value_cp',
                                'min_value_crr', 'min_value_cvh', 'min_value_cvl', 'min_value_e',
                                'min_value_lmlt', 'min_value_msl', 'min_value_ro', 'min_value_skt',
                                'min_value_sp', 'min_value_sro', 'min_value_swvl1', 'min_value_tcc',
                                'min_value_tcrw', 'min_value_tcwv', 'min_value_tp']:
                        if prev_row and row[key] and prev_row[key]:
                            row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                        else:
                            row[f'{key}_m_to_m_change'] = None

                    # Ambil data tahun sebelumnya di bulan yang sama
                    yoy_actual = previous_data_index.get((row['province'],row['city'], row['year'] - 1, row['month'], 'actual'))
                    yoy_predicted = previous_data_index.get((row['province'],row['city'], row['year'] - 1, row['month'], 'predicted'))
                    yoy_row = yoy_actual if yoy_actual else yoy_predicted

                    # Hitung perubahan Y-on-Y
                    for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                                'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                                'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                                'p_pk', 'p_mix', 'p_suspek_pk', 'p_others', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps','hujan_mean', 'hujan_max',
                                'hujan_min', 'rh_mean', 'rh_max', 'rh_min', 'tm_mean',
                                'tm_max', 'tm_min', 'max_value_10u', 'max_value_10v',
                                'max_value_2d', 'max_value_2t', 'max_value_cp', 'max_value_crr',
                                'max_value_cvh', 'max_value_cvl', 'max_value_e', 'max_value_lmlt',
                                'max_value_msl', 'max_value_ro', 'max_value_skt', 'max_value_sp',
                                'max_value_sro', 'max_value_swvl1', 'max_value_tcc', 'max_value_tcrw',
                                'max_value_tcwv', 'max_value_tp', 'mean_value_10u', 'mean_value_10v',
                                'mean_value_2d', 'mean_value_2t', 'mean_value_cp', 'mean_value_crr',
                                'mean_value_cvh', 'mean_value_cvl', 'mean_value_e', 'mean_value_lmlt',
                                'mean_value_msl', 'mean_value_ro', 'mean_value_skt', 'mean_value_sp',
                                'mean_value_sro', 'mean_value_swvl1', 'mean_value_tcc',
                                'mean_value_tcrw', 'mean_value_tcwv', 'mean_value_tp', 'min_value_10u',
                                'min_value_10v', 'min_value_2d', 'min_value_2t', 'min_value_cp',
                                'min_value_crr', 'min_value_cvh', 'min_value_cvl', 'min_value_e',
                                'min_value_lmlt', 'min_value_msl', 'min_value_ro', 'min_value_skt',
                                'min_value_sp', 'min_value_sro', 'min_value_swvl1', 'min_value_tcc',
                                'min_value_tcrw', 'min_value_tcwv', 'min_value_tp']:
                        if yoy_row and row[key] and yoy_row[key]:
                            row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                        else:
                            row[f'{key}_y_on_y_change'] = None

                    # Tambahkan hasil yang telah dihitung ke dalam final_data
                    final_data.append(row)

                return jsonify({"data": final_data, "success": True}), 200
        else:
            # SQL query untuk mengambil data prediksi
            query = text("""
            WITH combined_data AS (
                (
                    SELECT 
                        '00' as kd_prov,
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
                        SUM(p_others) as p_others,
                        SUM(penularan_indigenus) as penularan_indigenus,
                        SUM(penularan_impor) as penularan_impor,
                        SUM(penularan_induced) as penularan_induced,
                        SUM(relaps) as relaps
                    FROM 
                        malariamonthly mhfm
                    JOIN 
                        healthfacility hfi
                    ON 
                        mhfm.id_faskes = hfi.id_faskes
                    GROUP BY 
                        mhfm.tahun, mhfm.bulan, mhfm.status
                )
                UNION ALL
                (
                    SELECT 
                        hfi.kd_prov,
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
                        SUM(p_others) as p_others,
                        SUM(penularan_indigenus) as penularan_indigenus,
                        SUM(penularan_impor) as penularan_impor,
                        SUM(penularan_induced) as penularan_induced,
                        SUM(relaps) as relaps
                    FROM 
                        malariamonthly mhfm
                    JOIN 
                        (select hf.*,mp.provinsi from healthfacility hf left join masterprov mp on hf.kd_prov = mp.kd_prov) hfi
                    ON 
                        mhfm.id_faskes = hfi.id_faskes
                    GROUP BY 
                        hfi.kd_prov,hfi.provinsi, mhfm.tahun, mhfm.bulan, mhfm.status
                )
            ),
            aggregated_climate AS (
                (
                    -- Total average for all provinces (for the '00' code)
                    SELECT
                        '00' as kd_prov,
                        climatemonthly.tahun,
                        climatemonthly.bulan,
                        AVG(climatemonthly.hujan_hujan_mean) as hujan_mean,
                        AVG(climatemonthly.hujan_hujan_max) as hujan_max,
                        AVG(climatemonthly.hujan_hujan_min) as hujan_min,
                        AVG(climatemonthly.rh_rh_mean) as rh_mean,
                        AVG(climatemonthly.rh_rh_max) as rh_max,
                        AVG(climatemonthly.rh_rh_min) as rh_min,
                        AVG(climatemonthly.tm_tm_mean) as tm_mean,
                        AVG(climatemonthly.tm_tm_max) as tm_max,
                        AVG(climatemonthly.tm_tm_min) as tm_min,
                        AVG(climatemonthly.max_value_10u) as max_value_10u,
                        AVG(climatemonthly.max_value_10v) as max_value_10v,
                        AVG(climatemonthly.max_value_2d) as max_value_2d,
                        AVG(climatemonthly.max_value_2t) as max_value_2t,
                        AVG(climatemonthly.max_value_cp) as max_value_cp,
                        AVG(climatemonthly.max_value_crr) as max_value_crr,
                        AVG(climatemonthly.max_value_cvh) as max_value_cvh,
                        AVG(climatemonthly.max_value_cvl) as max_value_cvl,
                        AVG(climatemonthly.max_value_e) as max_value_e,
                        AVG(climatemonthly.max_value_lmlt) as max_value_lmlt,
                        AVG(climatemonthly.max_value_msl) as max_value_msl,
                        AVG(climatemonthly.max_value_ro) as max_value_ro,
                        AVG(climatemonthly.max_value_skt) as max_value_skt,
                        AVG(climatemonthly.max_value_sp) as max_value_sp,
                        AVG(climatemonthly.max_value_sro) as max_value_sro,
                        AVG(climatemonthly.max_value_swvl1) as max_value_swvl1,
                        AVG(climatemonthly.max_value_tcc) as max_value_tcc,
                        AVG(climatemonthly.max_value_tcrw) as max_value_tcrw,
                        AVG(climatemonthly.max_value_tcwv) as max_value_tcwv,
                        AVG(climatemonthly.max_value_tp) as max_value_tp,
                        AVG(climatemonthly.mean_value_10u) as mean_value_10u,
                        AVG(climatemonthly.mean_value_10v) as mean_value_10v,
                        AVG(climatemonthly.mean_value_2d) as mean_value_2d,
                        AVG(climatemonthly.mean_value_2t) as mean_value_2t,
                        AVG(climatemonthly.mean_value_cp) as mean_value_cp,
                        AVG(climatemonthly.mean_value_crr) as mean_value_crr,
                        AVG(climatemonthly.mean_value_cvh) as mean_value_cvh,
                        AVG(climatemonthly.mean_value_cvl) as mean_value_cvl,
                        AVG(climatemonthly.mean_value_e) as mean_value_e,
                        AVG(climatemonthly.mean_value_lmlt) as mean_value_lmlt,
                        AVG(climatemonthly.mean_value_msl) as mean_value_msl,
                        AVG(climatemonthly.mean_value_ro) as mean_value_ro,
                        AVG(climatemonthly.mean_value_skt) as mean_value_skt,
                        AVG(climatemonthly.mean_value_sp) as mean_value_sp,
                        AVG(climatemonthly.mean_value_sro) as mean_value_sro,
                        AVG(climatemonthly.mean_value_swvl1) as mean_value_swvl1,
                        AVG(climatemonthly.mean_value_tcc) as mean_value_tcc,
                        AVG(climatemonthly.mean_value_tcrw) as mean_value_tcrw,
                        AVG(climatemonthly.mean_value_tcwv) as mean_value_tcwv,
                        AVG(climatemonthly.mean_value_tp) as mean_value_tp,
                        AVG(climatemonthly.min_value_10u) as min_value_10u,
                        AVG(climatemonthly.min_value_10v) as min_value_10v,
                        AVG(climatemonthly.min_value_2d) as min_value_2d,
                        AVG(climatemonthly.min_value_2t) as min_value_2t,
                        AVG(climatemonthly.min_value_cp) as min_value_cp,
                        AVG(climatemonthly.min_value_crr) as min_value_crr,
                        AVG(climatemonthly.min_value_cvh) as min_value_cvh,
                        AVG(climatemonthly.min_value_cvl) as min_value_cvl,
                        AVG(climatemonthly.min_value_e) as min_value_e,
                        AVG(climatemonthly.min_value_lmlt) as min_value_lmlt,
                        AVG(climatemonthly.min_value_msl) as min_value_msl,
                        AVG(climatemonthly.min_value_ro) as min_value_ro,
                        AVG(climatemonthly.min_value_skt) as min_value_skt,
                        AVG(climatemonthly.min_value_sp) as min_value_sp,
                        AVG(climatemonthly.min_value_sro) as min_value_sro,
                        AVG(climatemonthly.min_value_swvl1) as min_value_swvl1,
                        AVG(climatemonthly.min_value_tcc) as min_value_tcc,
                        AVG(climatemonthly.min_value_tcrw) as min_value_tcrw,
                        AVG(climatemonthly.min_value_tcwv) as min_value_tcwv,
                        AVG(climatemonthly.min_value_tp) as min_value_tp
                    FROM 
                        climatemonthly
                    GROUP BY
                        climatemonthly.tahun, climatemonthly.bulan
                )
                UNION ALL
                (
                    -- Province-specific averages
                    SELECT
                        mkec.kd_prov,
                        climatemonthly.tahun,
                        climatemonthly.bulan,
                        AVG(climatemonthly.hujan_hujan_mean) as hujan_mean,
                        AVG(climatemonthly.hujan_hujan_max) as hujan_max,
                        AVG(climatemonthly.hujan_hujan_min) as hujan_min,
                        AVG(climatemonthly.rh_rh_mean) as rh_mean,
                        AVG(climatemonthly.rh_rh_max) as rh_max,
                        AVG(climatemonthly.rh_rh_min) as rh_min,
                        AVG(climatemonthly.tm_tm_mean) as tm_mean,
                        AVG(climatemonthly.tm_tm_max) as tm_max,
                        AVG(climatemonthly.tm_tm_min) as tm_min,
                        AVG(climatemonthly.max_value_10u) as max_value_10u,
                        AVG(climatemonthly.max_value_10v) as max_value_10v,
                        AVG(climatemonthly.max_value_2d) as max_value_2d,
                        AVG(climatemonthly.max_value_2t) as max_value_2t,
                        AVG(climatemonthly.max_value_cp) as max_value_cp,
                        AVG(climatemonthly.max_value_crr) as max_value_crr,
                        AVG(climatemonthly.max_value_cvh) as max_value_cvh,
                        AVG(climatemonthly.max_value_cvl) as max_value_cvl,
                        AVG(climatemonthly.max_value_e) as max_value_e,
                        AVG(climatemonthly.max_value_lmlt) as max_value_lmlt,
                        AVG(climatemonthly.max_value_msl) as max_value_msl,
                        AVG(climatemonthly.max_value_ro) as max_value_ro,
                        AVG(climatemonthly.max_value_skt) as max_value_skt,
                        AVG(climatemonthly.max_value_sp) as max_value_sp,
                        AVG(climatemonthly.max_value_sro) as max_value_sro,
                        AVG(climatemonthly.max_value_swvl1) as max_value_swvl1,
                        AVG(climatemonthly.max_value_tcc) as max_value_tcc,
                        AVG(climatemonthly.max_value_tcrw) as max_value_tcrw,
                        AVG(climatemonthly.max_value_tcwv) as max_value_tcwv,
                        AVG(climatemonthly.max_value_tp) as max_value_tp,
                        AVG(climatemonthly.mean_value_10u) as mean_value_10u,
                        AVG(climatemonthly.mean_value_10v) as mean_value_10v,
                        AVG(climatemonthly.mean_value_2d) as mean_value_2d,
                        AVG(climatemonthly.mean_value_2t) as mean_value_2t,
                        AVG(climatemonthly.mean_value_cp) as mean_value_cp,
                        AVG(climatemonthly.mean_value_crr) as mean_value_crr,
                        AVG(climatemonthly.mean_value_cvh) as mean_value_cvh,
                        AVG(climatemonthly.mean_value_cvl) as mean_value_cvl,
                        AVG(climatemonthly.mean_value_e) as mean_value_e,
                        AVG(climatemonthly.mean_value_lmlt) as mean_value_lmlt,
                        AVG(climatemonthly.mean_value_msl) as mean_value_msl,
                        AVG(climatemonthly.mean_value_ro) as mean_value_ro,
                        AVG(climatemonthly.mean_value_skt) as mean_value_skt,
                        AVG(climatemonthly.mean_value_sp) as mean_value_sp,
                        AVG(climatemonthly.mean_value_sro) as mean_value_sro,
                        AVG(climatemonthly.mean_value_swvl1) as mean_value_swvl1,
                        AVG(climatemonthly.mean_value_tcc) as mean_value_tcc,
                        AVG(climatemonthly.mean_value_tcrw) as mean_value_tcrw,
                        AVG(climatemonthly.mean_value_tcwv) as mean_value_tcwv,
                        AVG(climatemonthly.mean_value_tp) as mean_value_tp,
                        AVG(climatemonthly.min_value_10u) as min_value_10u,
                        AVG(climatemonthly.min_value_10v) as min_value_10v,
                        AVG(climatemonthly.min_value_2d) as min_value_2d,
                        AVG(climatemonthly.min_value_2t) as min_value_2t,
                        AVG(climatemonthly.min_value_cp) as min_value_cp,
                        AVG(climatemonthly.min_value_crr) as min_value_crr,
                        AVG(climatemonthly.min_value_cvh) as min_value_cvh,
                        AVG(climatemonthly.min_value_cvl) as min_value_cvl,
                        AVG(climatemonthly.min_value_e) as min_value_e,
                        AVG(climatemonthly.min_value_lmlt) as min_value_lmlt,
                        AVG(climatemonthly.min_value_msl) as min_value_msl,
                        AVG(climatemonthly.min_value_ro) as min_value_ro,
                        AVG(climatemonthly.min_value_skt) as min_value_skt,
                        AVG(climatemonthly.min_value_sp) as min_value_sp,
                        AVG(climatemonthly.min_value_sro) as min_value_sro,
                        AVG(climatemonthly.min_value_swvl1) as min_value_swvl1,
                        AVG(climatemonthly.min_value_tcc) as min_value_tcc,
                        AVG(climatemonthly.min_value_tcrw) as min_value_tcrw,
                        AVG(climatemonthly.min_value_tcwv) as min_value_tcwv,
                        AVG(climatemonthly.min_value_tp) as min_value_tp
                    FROM 
                        climatemonthly
                    JOIN
                        masterkec mkec
                    ON 
                        climatemonthly.kd_kec = mkec.kd_kec
                    GROUP BY
                        mkec.kd_prov, climatemonthly.tahun, climatemonthly.bulan
                )
            )
            SELECT 
                cd.*,
                ac.hujan_mean,
                ac.hujan_max,
                ac.hujan_min,
                ac.rh_mean,
                ac.rh_max,
                ac.rh_min,
                ac.tm_mean,
                ac.tm_max,
                ac.tm_min,
                ac.max_value_10u,
                ac.max_value_10v,
                ac.max_value_2d,
                ac.max_value_2t,
                ac.max_value_cp,
                ac.max_value_crr,
                ac.max_value_cvh,
                ac.max_value_cvl,
                ac.max_value_e,
                ac.max_value_lmlt,
                ac.max_value_msl,
                ac.max_value_ro,
                ac.max_value_skt,
                ac.max_value_sp,
                ac.max_value_sro,
                ac.max_value_swvl1,
                ac.max_value_tcc,
                ac.max_value_tcrw,
                ac.max_value_tcwv,
                ac.max_value_tp,
                ac.mean_value_10u,
                ac.mean_value_10v,
                ac.mean_value_2d,
                ac.mean_value_2t,
                ac.mean_value_cp,
                ac.mean_value_crr,
                ac.mean_value_cvh,
                ac.mean_value_cvl,
                ac.mean_value_e,
                ac.mean_value_lmlt,
                ac.mean_value_msl,
                ac.mean_value_ro,
                ac.mean_value_skt,
                ac.mean_value_sp,
                ac.mean_value_sro,
                ac.mean_value_swvl1,
                ac.mean_value_tcc,
                ac.mean_value_tcrw,
                ac.mean_value_tcwv,
                ac.mean_value_tp,
                ac.min_value_10u,
                ac.min_value_10v,
                ac.min_value_2d,
                ac.min_value_2t,
                ac.min_value_cp,
                ac.min_value_crr,
                ac.min_value_cvh,
                ac.min_value_cvl,
                ac.min_value_e,
                ac.min_value_lmlt,
                ac.min_value_msl,
                ac.min_value_ro,
                ac.min_value_skt,
                ac.min_value_sp,
                ac.min_value_sro,
                ac.min_value_swvl1,
                ac.min_value_tcc,
                ac.min_value_tcrw,
                ac.min_value_tcwv,
                ac.min_value_tp
            FROM 
                combined_data cd
            LEFT JOIN
                aggregated_climate ac
            ON 
                cd.kd_prov = ac.kd_prov
                AND cd.year = ac.tahun
                AND cd.month = ac.bulan
            WHERE 
                cd.kd_prov = :province
            """)

            # Tambahkan kondisi untuk rentang tanggal
            if start_month_year and end_month_year:
                query = text(str(query) + date_range_condition)

            query = text(str(query) + " ORDER BY province, year, month;")

            # Eksekusi query menggunakan SQLAlchemy
            params = {'province': province}
            if start_month_year and end_month_year:
                params['start_date_value'] = start_date_value
                params['end_date_value'] = end_date_value

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
                if row['month'] == 1:  # January - look at previous year's December
                    prev_actual = previous_data_index.get((row['province'], row['year'] - 1, 12, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['year'] - 1, 12, 'predicted'))
                else:
                    prev_actual = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'predicted'))

                prev_row = prev_actual if prev_actual else prev_predicted

                # Hitung perubahan M-to-M
                for key in ['tot_pos', 'konfirmasi_lab_mikroskop', 'konfirmasi_lab_rdt', 'konfirmasi_lab_pcr',
                            'pos_0_4', 'pos_5_14', 'pos_15_64', 'pos_diatas_64', 'hamil_pos', 'kematian_malaria',
                            'obat_standar', 'obat_nonprogram', 'obat_primaquin', 'p_pf', 'p_pv', 'p_po', 'p_pm',
                            'p_pk', 'p_mix', 'p_suspek_pk', 'p_others','penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps','hujan_mean', 'hujan_max',
                                'hujan_min', 'rh_mean', 'rh_max', 'rh_min', 'tm_mean',
                                'tm_max', 'tm_min', 'max_value_10u', 'max_value_10v',
                                'max_value_2d', 'max_value_2t', 'max_value_cp', 'max_value_crr',
                                'max_value_cvh', 'max_value_cvl', 'max_value_e', 'max_value_lmlt',
                                'max_value_msl', 'max_value_ro', 'max_value_skt', 'max_value_sp',
                                'max_value_sro', 'max_value_swvl1', 'max_value_tcc', 'max_value_tcrw',
                                'max_value_tcwv', 'max_value_tp', 'mean_value_10u', 'mean_value_10v',
                                'mean_value_2d', 'mean_value_2t', 'mean_value_cp', 'mean_value_crr',
                                'mean_value_cvh', 'mean_value_cvl', 'mean_value_e', 'mean_value_lmlt',
                                'mean_value_msl', 'mean_value_ro', 'mean_value_skt', 'mean_value_sp',
                                'mean_value_sro', 'mean_value_swvl1', 'mean_value_tcc',
                                'mean_value_tcrw', 'mean_value_tcwv', 'mean_value_tp', 'min_value_10u',
                                'min_value_10v', 'min_value_2d', 'min_value_2t', 'min_value_cp',
                                'min_value_crr', 'min_value_cvh', 'min_value_cvl', 'min_value_e',
                                'min_value_lmlt', 'min_value_msl', 'min_value_ro', 'min_value_skt',
                                'min_value_sp', 'min_value_sro', 'min_value_swvl1', 'min_value_tcc',
                                'min_value_tcrw', 'min_value_tcwv', 'min_value_tp']:
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
                            'p_pk', 'p_mix', 'p_suspek_pk', 'p_others', 'penularan_indigenus', 'penularan_impor', 'penularan_induced', 'relaps','hujan_mean', 'hujan_max',
                                'hujan_min', 'rh_mean', 'rh_max', 'rh_min', 'tm_mean',
                                'tm_max', 'tm_min', 'max_value_10u', 'max_value_10v',
                                'max_value_2d', 'max_value_2t', 'max_value_cp', 'max_value_crr',
                                'max_value_cvh', 'max_value_cvl', 'max_value_e', 'max_value_lmlt',
                                'max_value_msl', 'max_value_ro', 'max_value_skt', 'max_value_sp',
                                'max_value_sro', 'max_value_swvl1', 'max_value_tcc', 'max_value_tcrw',
                                'max_value_tcwv', 'max_value_tp', 'mean_value_10u', 'mean_value_10v',
                                'mean_value_2d', 'mean_value_2t', 'mean_value_cp', 'mean_value_crr',
                                'mean_value_cvh', 'mean_value_cvl', 'mean_value_e', 'mean_value_lmlt',
                                'mean_value_msl', 'mean_value_ro', 'mean_value_skt', 'mean_value_sp',
                                'mean_value_sro', 'mean_value_swvl1', 'mean_value_tcc',
                                'mean_value_tcrw', 'mean_value_tcwv', 'mean_value_tp', 'min_value_10u',
                                'min_value_10v', 'min_value_2d', 'min_value_2t', 'min_value_cp',
                                'min_value_crr', 'min_value_cvh', 'min_value_cvl', 'min_value_e',
                                'min_value_lmlt', 'min_value_msl', 'min_value_ro', 'min_value_skt',
                                'min_value_sp', 'min_value_sro', 'min_value_swvl1', 'min_value_tcc',
                                'min_value_tcrw', 'min_value_tcwv', 'min_value_tp']:
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

@bp.route('/get-raw-data-dbd', methods=['GET'])
@jwt_required()
def get_raw_data_dbd():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        
        if province == '00':
            province = None
        
        # Ambil parameter month_year jika ada
        month_year = request.args.get('month_year', default=None, type=str)
        month = None
        year = None
        
        if month_year:
            # Format yang diharapkan: "YYYY-MM"
            try:
                year, month = map(int, month_year.split('-'))
            except (ValueError, IndexError):
                return jsonify({"error": "Format month_year harus YYYY-MM"}), 400
        
        # SQL query untuk periode saat ini - gunakan nama kolom yang sesuai dengan model
        current_query = text("""
            SELECT 
                mwk.kd_prov,
                mwk.kd_kab,
                mwk.provinsi AS province,
                mwk.kabkot AS city,
                dk.tahun AS year,
                dk.bulan AS month,
                dk."DBD_P" AS dbd_p,
                dk."DBD_M" AS dbd_m,
                dk.status
            FROM 
                dbd dk
            JOIN 
                (select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                    from masterkab mk
                    left join masterprov mp 
                    on mk.kd_prov = mp.kd_prov) mwk ON dk.kd_kab = mwk.kd_kab
        """)
        
        # Build the WHERE clause
        where_clauses = []
        params = {}
        
        if province:
            where_clauses.append("mwk.kd_prov = :province")
            params['province'] = province
            
        if city:
            where_clauses.append("mwk.kd_kab = :city")
            params['city'] = city
            
        # Add WHERE clause to the query if any conditions exist
        if where_clauses:
            current_query = text(str(current_query) + " WHERE " + " AND ".join(where_clauses))
        
        # Eksekusi query
        current_data = db.session.execute(current_query, params).fetchall()
        
        # Konversi ke dictionary untuk kemudahan manipulasi
        current_data = [row._asdict() for row in current_data]
        previous_data_index = {
            (row['kd_kab'], row['year'], row['month']): row
            for row in current_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        for row in current_data:
            # Check if current month is January to properly handle month-to-month change
            if row['month'] == 1:
                # For January, look at December of previous year
                prev_row = previous_data_index.get((row['kd_kab'], row['year'] - 1, 12))
            else:
                # For all other months, look at previous month in same year
                prev_row = previous_data_index.get((row['kd_kab'], row['year'], row['month'] - 1))
            
            for key in ['dbd_p', 'dbd_m']:
                if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                    row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    row[f'{key}_m_to_m_change'] = None

                yoy_row = previous_data_index.get((row['kd_kab'], row['year'] - 1, row['month']))
                if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                    row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    row[f'{key}_y_on_y_change'] = None

            # Tambahkan hasil yang telah dihitung ke dalam final_data
            final_data.append(row)
        
        # Langkah 1: Cari tahun dan bulan terakhir yang status="actual"
        actual_data = [row for row in final_data if row['status'] == 'actual']
        if not actual_data:
            return jsonify({"error": "No actual data found"}), 404
            
        max_year_actual = max(row['year'] for row in actual_data)
        max_month_actual = max(row['month'] for row in actual_data if row['year'] == max_year_actual)
        
        # Tentukan bulan prediksi pertama setelah actual terakhir
        if max_month_actual == 12:
            next_month_predicted = 1
            next_year_predicted = max_year_actual + 1
        else:
            next_month_predicted = max_month_actual + 1
            next_year_predicted = max_year_actual
        
        # Jika month_year tidak disediakan, default ke bulan dan tahun pertama dengan data predicted
        if month is None or year is None:
            month = next_month_predicted
            year = next_year_predicted
        
        # Filter data berdasarkan bulan dan tahun yang dipilih
        filtered_data = [
            row for row in final_data 
            if row['year'] == year and row['month'] == month
        ]
        
        # Tambahkan deduplication berdasarkan kd_kab jika diperlukan
        unique_data = []
        seen = set()
        for row in filtered_data:
            if row['kd_kab'] not in seen:
                unique_data.append(row)
                seen.add(row['kd_kab'])
        
        return jsonify({
            "data": unique_data, 
            "metadata": {
                "current_filter": {
                    "year": year,
                    "month": month,
                    "status": unique_data[0]['status'] if unique_data else None
                },
                "last_actual": {
                    "year": max_year_actual,
                    "month": max_month_actual
                },
                "first_predicted": {
                    "year": next_year_predicted,
                    "month": next_month_predicted
                }
            },
            "success": True
        }), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-aggregate-data-dbd', methods=['GET'])
@jwt_required()
def get_aggregate_data_dbd():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        start_month_year = request.args.get('start', default=None, type=str)
        end_month_year = request.args.get('end', default=None, type=str)
        
        if not province:
            return jsonify({"error": "Parameter 'province' is required"}), 400
            
        # Parse start and end dates if provided
        date_range_condition = ""
        if start_month_year and end_month_year:
            # Parse start and end dates dari format "2021-1"
            start_parts = start_month_year.split('-')
            start_year = int(start_parts[0])
            start_month = int(start_parts[1])

            end_parts = end_month_year.split('-')
            end_year = int(end_parts[0])
            end_month = int(end_parts[1])

            # Calculate date values untuk perbandingan (format: YYYYMM)
            start_date_value = start_year * 100 + start_month
            end_date_value = end_year * 100 + end_month
            
            # FIX: Reference the aliased columns from the combined_data CTE
            date_range_condition = " AND (year * 100 + month) >= :start_date_value AND (year * 100 + month) <= :end_date_value"
        
        if city:
            query = text("""
            WITH combined_data AS (
            SELECT 
                hfi.kd_prov, hfi.kd_kab,
                hfi.provinsi AS province,
                hfi.kabkot AS city,
                dk.tahun AS year,
                dk.bulan AS month,
                dk.status AS status,
                SUM(dk."DBD_P") AS dbd_p,
                SUM(dk."DBD_M") AS dbd_m
            FROM 
                dbd dk
            JOIN 
                (
                select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                from masterkab mk
                left join masterprov mp 
                on mk.kd_prov = mp.kd_prov) hfi
            ON 
                dk.kd_kab = hfi.kd_kab
            GROUP BY 
                hfi.kd_prov, hfi.kd_kab, hfi.provinsi, hfi.kabkot, dk.tahun, dk.bulan, dk.status
            )
            SELECT 
                *
            FROM 
                combined_data
            WHERE 
                kd_prov = :province and kd_kab = :city
            """)

            # Tambahkan kondisi untuk rentang tanggal
            if start_month_year and end_month_year:
                query = text(str(query) + date_range_condition)

            query = text(str(query) + " ORDER BY province, city, year, month;")

            # Eksekusi query menggunakan SQLAlchemy
            params = {'province': province, 
                      'city': city}
            if start_month_year and end_month_year:
                params['start_date_value'] = start_date_value
                params['end_date_value'] = end_date_value

            current_data = db.session.execute(query, params).fetchall()

            # Konversi ke dictionary untuk kemudahan manipulasi
            current_data = [row._asdict() for row in current_data]
            previous_data_index = {
                (row['province'], row['city'], row['year'], row['month'], row['status']): row
                for row in current_data
            }

            # Hitung perubahan M-to-M dan Y-on-Y
            final_data = []
            for row in current_data:
                # Ambil data bulan sebelumnya
                if row['month'] == 1:  # January - look at previous year's December
                    prev_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'predicted'))
                else:
                    prev_actual = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'predicted'))

                prev_row = prev_actual if prev_actual else prev_predicted

                # Hitung perubahan M-to-M
                for key in ['dbd_p', 'dbd_m']:
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['dbd_p', 'dbd_m']:
                    if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                        row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                    else:
                        row[f'{key}_y_on_y_change'] = None

                # Tambahkan hasil yang telah dihitung ke dalam final_data
                final_data.append(row)

            return jsonify({"data": final_data, "success": True}), 200
        else:
            # SQL query untuk mengambil data prediksi
            query = text("""
            WITH combined_data AS (
                (
                    SELECT 
                        '00' as kd_prov,
                        'TOTAL' AS province,
                        dk.tahun AS year,
                        dk.bulan AS month,
                        dk.status AS status,
                        SUM(dk."DBD_P") AS dbd_p,
                        SUM(dk."DBD_M") AS dbd_m
                    FROM 
                        dbd dk
                    JOIN 
                        (select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                        from masterkab mk
                        left join masterprov mp 
                        on mk.kd_prov = mp.kd_prov) hfi
                    ON 
                        dk.kd_kab = hfi.kd_kab
                    GROUP BY 
                        dk.tahun, dk.bulan, dk.status
                )
                UNION ALL
                (
                    SELECT 
                        hfi.kd_prov,
                        hfi.provinsi AS province,
                        dk.tahun AS year,
                        dk.bulan AS month,
                        dk.status AS status,
                        SUM(dk."DBD_P") AS dbd_p,
                        SUM(dk."DBD_M") AS dbd_m
                    FROM 
                        dbd dk
                    JOIN 
                        (
                         select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                        from masterkab mk
                        left join masterprov mp 
                        on mk.kd_prov = mp.kd_prov) hfi
                    ON 
                        dk.kd_kab = hfi.kd_kab
                    GROUP BY 
                        hfi.kd_prov, hfi.provinsi, dk.tahun, dk.bulan, dk.status
                )
            )
            SELECT 
                *
            FROM 
                combined_data
            WHERE 
                kd_prov = :province
            """)

            # Tambahkan kondisi untuk rentang tanggal
            if start_month_year and end_month_year:
                query = text(str(query) + date_range_condition)

            query = text(str(query) + " ORDER BY province, year, month;")

            # Eksekusi query menggunakan SQLAlchemy
            params = {'province': province}
            if start_month_year and end_month_year:
                params['start_date_value'] = start_date_value
                params['end_date_value'] = end_date_value

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
                if row['month'] == 1:  # January - look at previous year's December
                    prev_actual = previous_data_index.get((row['province'], row['year'] - 1, 12, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['year'] - 1, 12, 'predicted'))
                else:
                    prev_actual = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'predicted'))

                prev_row = prev_actual if prev_actual else prev_predicted

                # Hitung perubahan M-to-M
                for key in ['dbd_p', 'dbd_m']:
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['dbd_p', 'dbd_m']:
                    if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                        row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                    else:
                        row[f'{key}_y_on_y_change'] = None

                # Tambahkan hasil yang telah dihitung ke dalam final_data
                final_data.append(row)

            return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-raw-data-lepto', methods=['GET'])
@jwt_required()
def get_raw_data_lepto():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        
        if province == '00':
            province = None
        
        # Ambil parameter month_year jika ada
        month_year = request.args.get('month_year', default=None, type=str)
        month = None
        year = None
        
        if month_year:
            # Format yang diharapkan: "YYYY-MM"
            try:
                year, month = map(int, month_year.split('-'))
            except (ValueError, IndexError):
                return jsonify({"error": "Format month_year harus YYYY-MM"}), 400
        
        # SQL query untuk periode saat ini - gunakan nama kolom yang sesuai dengan model
        current_query = text("""
            SELECT 
                mwk.kd_prov,
                mwk.kd_kab,
                mwk.provinsi AS province,
                mwk.kabkot AS city,
                lk.tahun AS year,
                lk.bulan AS month,
                lk."LEP_K" AS lep_k,
                lk."LEP_M" AS lep_m,
                lk.status
            FROM 
                lepto lk
            JOIN 
                (select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                    from masterkab mk
                    left join masterprov mp 
                    on mk.kd_prov = mp.kd_prov) mwk ON lk.kd_kab = mwk.kd_kab
        """)
        
        # Build the WHERE clause
        where_clauses = []
        params = {}
        
        if province:
            where_clauses.append("mwk.kd_prov = :province")
            params['province'] = province
            
        if city:
            where_clauses.append("mwk.kd_kab = :city")
            params['city'] = city
            
        # Add WHERE clause to the query if any conditions exist
        if where_clauses:
            current_query = text(str(current_query) + " WHERE " + " AND ".join(where_clauses))
        
        # Eksekusi query
        current_data = db.session.execute(current_query, params).fetchall()
        
        # Konversi ke dictionary untuk kemudahan manipulasi
        current_data = [row._asdict() for row in current_data]
        previous_data_index = {
            (row['kd_kab'], row['year'], row['month']): row
            for row in current_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        for row in current_data:
            # Check if current month is January to properly handle month-to-month change
            if row['month'] == 1:
                # For January, look at December of previous year
                prev_row = previous_data_index.get((row['kd_kab'], row['year'] - 1, 12))
            else:
                # For all other months, look at previous month in same year
                prev_row = previous_data_index.get((row['kd_kab'], row['year'], row['month'] - 1))
            
            for key in ['lep_k', 'lep_m']:
                if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                    row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    row[f'{key}_m_to_m_change'] = None

                yoy_row = previous_data_index.get((row['kd_kab'], row['year'] - 1, row['month']))
                if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                    row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    row[f'{key}_y_on_y_change'] = None

            # Tambahkan hasil yang telah dihitung ke dalam final_data
            final_data.append(row)
        
        # Langkah 1: Cari tahun dan bulan terakhir yang status="actual"
        actual_data = [row for row in final_data if row['status'] == 'actual']
        if not actual_data:
            return jsonify({"error": "No actual data found"}), 404
            
        max_year_actual = max(row['year'] for row in actual_data)
        max_month_actual = max(row['month'] for row in actual_data if row['year'] == max_year_actual)
        
        # Tentukan bulan prediksi pertama setelah actual terakhir
        if max_month_actual == 12:
            next_month_predicted = 1
            next_year_predicted = max_year_actual + 1
        else:
            next_month_predicted = max_month_actual + 1
            next_year_predicted = max_year_actual
        
        # Jika month_year tidak disediakan, default ke bulan dan tahun pertama dengan data predicted
        if month is None or year is None:
            month = next_month_predicted
            year = next_year_predicted
        
        # Filter data berdasarkan bulan dan tahun yang dipilih
        filtered_data = [
            row for row in final_data 
            if row['year'] == year and row['month'] == month
        ]
        
        # Tambahkan deduplication berdasarkan kd_kab jika diperlukan
        unique_data = []
        seen = set()
        for row in filtered_data:
            if row['kd_kab'] not in seen:
                unique_data.append(row)
                seen.add(row['kd_kab'])
        
        return jsonify({
            "data": unique_data, 
            "metadata": {
                "current_filter": {
                    "year": year,
                    "month": month,
                    "status": unique_data[0]['status'] if unique_data else None
                },
                "last_actual": {
                    "year": max_year_actual,
                    "month": max_month_actual
                },
                "first_predicted": {
                    "year": next_year_predicted,
                    "month": next_month_predicted
                }
            },
            "success": True
        }), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()

@bp.route('/get-aggregate-data-lepto', methods=['GET'])
@jwt_required()
def get_aggregate_data_lepto():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        start_month_year = request.args.get('start', default=None, type=str)
        end_month_year = request.args.get('end', default=None, type=str)
        
        if not province:
            return jsonify({"error": "Parameter 'province' is required"}), 400
            
        # Parse start and end dates if provided
        date_range_condition = ""
        if start_month_year and end_month_year:
            # Parse start and end dates dari format "2021-1"
            start_parts = start_month_year.split('-')
            start_year = int(start_parts[0])
            start_month = int(start_parts[1])

            end_parts = end_month_year.split('-')
            end_year = int(end_parts[0])
            end_month = int(end_parts[1])

            # Calculate date values untuk perbandingan (format: YYYYMM)
            start_date_value = start_year * 100 + start_month
            end_date_value = end_year * 100 + end_month
            
            # FIX: Reference the aliased columns from the combined_data CTE
            date_range_condition = " AND (year * 100 + month) >= :start_date_value AND (year * 100 + month) <= :end_date_value"
        
        if city:
            query = text("""
            WITH combined_data AS (
            SELECT 
                hfi.kd_prov, hfi.kd_kab,
                hfi.provinsi AS province,
                hfi.kabkot AS city,
                lk.tahun AS year,
                lk.bulan AS month,
                lk.status AS status,
                SUM(lk."LEP_K") AS lep_k,
                SUM(lk."LEP_M") AS lep_m
            FROM 
                lepto lk
            JOIN 
                (
                select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                from masterkab mk
                left join masterprov mp 
                on mk.kd_prov = mp.kd_prov) hfi
            ON 
                lk.kd_kab = hfi.kd_kab
            GROUP BY 
                hfi.kd_prov, hfi.kd_kab, hfi.provinsi, hfi.kabkot, lk.tahun, lk.bulan, lk.status
            )
            SELECT 
                *
            FROM 
                combined_data
            WHERE 
                kd_prov = :province and kd_kab = :city
            """)

            # Tambahkan kondisi untuk rentang tanggal
            if start_month_year and end_month_year:
                query = text(str(query) + date_range_condition)

            query = text(str(query) + " ORDER BY province, city, year, month;")

            # Eksekusi query menggunakan SQLAlchemy
            params = {'province': province, 
                      'city': city}
            if start_month_year and end_month_year:
                params['start_date_value'] = start_date_value
                params['end_date_value'] = end_date_value

            current_data = db.session.execute(query, params).fetchall()

            # Konversi ke dictionary untuk kemudahan manipulasi
            current_data = [row._asdict() for row in current_data]
            previous_data_index = {
                (row['province'], row['city'], row['year'], row['month'], row['status']): row
                for row in current_data
            }

            # Hitung perubahan M-to-M dan Y-on-Y
            final_data = []
            for row in current_data:
                # Ambil data bulan sebelumnya
                if row['month'] == 1:  # January - look at previous year's December
                    prev_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'predicted'))
                else:
                    prev_actual = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'predicted'))

                prev_row = prev_actual if prev_actual else prev_predicted

                # Hitung perubahan M-to-M
                for key in ['lep_k', 'lep_m']:
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['lep_k', 'lep_m']:
                    if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                        row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                    else:
                        row[f'{key}_y_on_y_change'] = None

                # Tambahkan hasil yang telah dihitung ke dalam final_data
                final_data.append(row)

            return jsonify({"data": final_data, "success": True}), 200
        else:
            # SQL query untuk mengambil data prediksi
            query = text("""
            WITH combined_data AS (
                (
                    SELECT 
                        '00' as kd_prov,
                        'TOTAL' AS province,
                        lk.tahun AS year,
                        lk.bulan AS month,
                        lk.status AS status,
                        SUM(lk."LEP_K") AS lep_k,
                        SUM(lk."LEP_M") AS lep_m
                    FROM 
                        lepto lk
                    JOIN 
                        (select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                        from masterkab mk
                        left join masterprov mp 
                        on mk.kd_prov = mp.kd_prov) hfi
                    ON 
                        lk.kd_kab = hfi.kd_kab
                    GROUP BY 
                        lk.tahun, lk.bulan, lk.status
                )
                UNION ALL
                (
                    SELECT 
                        hfi.kd_prov,
                        hfi.provinsi AS province,
                        lk.tahun AS year,
                        lk.bulan AS month,
                        lk.status AS status,
                        SUM(lk."LEP_K") AS lep_k,
                        SUM(lk."LEP_M") AS lep_m
                    FROM 
                        lepto lk
                    JOIN 
                        (
                         select mp.provinsi, mk.kabkot, mk.kd_kab, mk.kd_prov
                        from masterkab mk
                        left join masterprov mp 
                        on mk.kd_prov = mp.kd_prov) hfi
                    ON 
                        lk.kd_kab = hfi.kd_kab
                    GROUP BY 
                        hfi.kd_prov, hfi.provinsi, lk.tahun, lk.bulan, lk.status
                )
            )
            SELECT 
                *
            FROM 
                combined_data
            WHERE 
                kd_prov = :province
            """)

            # Tambahkan kondisi untuk rentang tanggal
            if start_month_year and end_month_year:
                query = text(str(query) + date_range_condition)

            query = text(str(query) + " ORDER BY province, year, month;")

            # Eksekusi query menggunakan SQLAlchemy
            params = {'province': province}
            if start_month_year and end_month_year:
                params['start_date_value'] = start_date_value
                params['end_date_value'] = end_date_value

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
                if row['month'] == 1:  # January - look at previous year's December
                    prev_actual = previous_data_index.get((row['province'], row['year'] - 1, 12, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['year'] - 1, 12, 'predicted'))
                else:
                    prev_actual = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'actual'))
                    prev_predicted = previous_data_index.get((row['province'], row['year'], row['month'] - 1, 'predicted'))

                prev_row = prev_actual if prev_actual else prev_predicted

                # Hitung perubahan M-to-M
                for key in ['lep_k', 'lep_m']:
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['lep_k', 'lep_m']:
                    if yoy_row and row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                        row[f'{key}_y_on_y_change'] = ((row[key] - yoy_row[key]) / yoy_row[key]) * 100
                    else:
                        row[f'{key}_y_on_y_change'] = None

                # Tambahkan hasil yang telah dihitung ke dalam final_data
                final_data.append(row)

            return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan, silahkan coba lagi"}), 500
    finally:
        db.session.close()