
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
import re

bp = Blueprint('data', __name__, url_prefix='')

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
                "status": row.status
            })
        
        previous_data_index = {
                    (row['province'], row['city'], row['year'], row['month'], row['status']): row
                    for row in malaria_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        
        for row in malaria_data:
            # Buat salinan dari row yang akan dimodifikasi
            new_row = row.copy()
            
            # Ambil data bulan sebelumnya
            if row['month'] == 1:  # January - look at previous year's December
                prev_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'actual'))
                prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'predicted'))
            else:
                prev_actual = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'actual'))
                prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'predicted'))

            prev_row = prev_actual if prev_actual else prev_predicted

            # Ambil data tahun sebelumnya di bulan yang sama
            yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
            yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
            yoy_row = yoy_actual if yoy_actual else yoy_predicted

            # Hitung perubahan M-to-M dan Y-on-Y untuk semua metrik yang diperlukan
            for key in ['predicted_penularan_indigenus', 'predicted_tot_pos']:
                # M-to-M change
                if prev_row and new_row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                    new_row[f'{key}_m_to_m_change'] = ((new_row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    new_row[f'{key}_m_to_m_change'] = None
                
                # Y-on-Y change
                if yoy_row and new_row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                    new_row[f'{key}_y_on_y_change'] = ((new_row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    new_row[f'{key}_y_on_y_change'] = None
            
            # Tambahkan row yang telah diperbarui ke final_data
            final_data.append(new_row)
        
        return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()

@bp.route('/get-warning-lepto', methods=['GET'])
@jwt_required()
def get_warning_lepto():
    try:
        query = text("""
        SELECT *
        FROM
        (SELECT hfi.kd_kab,hfi.kabkot,hfi.status_endemis,
        hfi.kd_prov,hfi.provinsi,bulan as month,tahun as year,status,sum("LEP_K") as predicted_lep_k,sum("LEP_M") as predicted_lep_m
        FROM lepto
        JOIN 
                                (
                                select mp.provinsi,mk.*
                                from masterkab mk
                                left join masterprov mp 
                                on mk.kd_prov = mp.kd_prov) hfi
        ON lepto.kd_kab = hfi.kd_kab
        GROUP BY hfi.kd_kab,hfi.kabkot,hfi.kd_prov,hfi.provinsi,status,bulan,tahun,status_endemis)
    """)
        # Eksekusi query
        result = db.session.execute(query).fetchall()
        # Format hasil query sesuai dengan data yang diambil
        lepto_data = []
        for row in result:
            lepto_data.append({
                "kd_kab": row.kd_kab,
                "city": row.kabkot,
                "status_endemis": row.status_endemis,
                "kd_prov": row.kd_prov,
                "province": row.provinsi,
                "month": row.month,
                "year": row.year,
                "predicted_lep_k": row.predicted_lep_k,
                "predicted_lep_m": row.predicted_lep_m,
                "status": row.status
            })
        
        previous_data_index = {
                    (row['province'], row['city'], row['year'], row['month'], row['status']): row
                    for row in lepto_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        
        for row in lepto_data:
            # Buat salinan dari row yang akan dimodifikasi
            new_row = row.copy()
            
            # Ambil data bulan sebelumnya
            if row['month'] == 1:  # January - look at previous year's December
                prev_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'actual'))
                prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'predicted'))
            else:
                prev_actual = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'actual'))
                prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'predicted'))

            prev_row = prev_actual if prev_actual else prev_predicted

            # Ambil data tahun sebelumnya di bulan yang sama
            yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
            yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
            yoy_row = yoy_actual if yoy_actual else yoy_predicted

            # Hitung perubahan M-to-M dan Y-on-Y untuk semua metrik yang diperlukan
            for key in ['predicted_lep_k', 'predicted_lep_m']:
                # M-to-M change
                if prev_row and new_row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                    new_row[f'{key}_m_to_m_change'] = ((new_row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    new_row[f'{key}_m_to_m_change'] = None
                
                # Y-on-Y change
                if yoy_row and new_row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                    new_row[f'{key}_y_on_y_change'] = ((new_row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    new_row[f'{key}_y_on_y_change'] = None
            
            # Tambahkan row yang telah diperbarui ke final_data
            final_data.append(new_row)
        
        return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()

@bp.route('/get-warning-dbd', methods=['GET'])
@jwt_required()
def get_warning_dbd():
    try:
        query = text("""
        SELECT *
        FROM
        (SELECT hfi.kd_kab,hfi.kabkot,hfi.status_endemis,
        hfi.kd_prov,hfi.provinsi,bulan as month,tahun as year,status,sum("DBD_P") as predicted_dbd_p,sum("DBD_M") as predicted_dbd_m
        FROM dbd
        JOIN 
                                (
                                select mp.provinsi,mk.*
                                from masterkab mk
                                left join masterprov mp 
                                on mk.kd_prov = mp.kd_prov) hfi
        ON dbd.kd_kab = hfi.kd_kab
        GROUP BY hfi.kd_kab,hfi.kabkot,hfi.kd_prov,hfi.provinsi,status,bulan,tahun,status_endemis)
    """)
        # Eksekusi query
        result = db.session.execute(query).fetchall()
        # Format hasil query sesuai dengan data yang diambil
        lepto_data = []
        for row in result:
            lepto_data.append({
                "kd_kab": row.kd_kab,
                "city": row.kabkot,
                "status_endemis": row.status_endemis,
                "kd_prov": row.kd_prov,
                "province": row.provinsi,
                "month": row.month,
                "year": row.year,
                "predicted_dbd_p": row.predicted_dbd_p,
                "predicted_dbd_m": row.predicted_dbd_m,
                "status": row.status
            })
        
        previous_data_index = {
                    (row['province'], row['city'], row['year'], row['month'], row['status']): row
                    for row in lepto_data
        }

        # Hitung perubahan M-to-M dan Y-on-Y
        final_data = []
        
        for row in lepto_data:
            # Buat salinan dari row yang akan dimodifikasi
            new_row = row.copy()
            
            # Ambil data bulan sebelumnya
            if row['month'] == 1:  # January - look at previous year's December
                prev_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'actual'))
                prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, 12, 'predicted'))
            else:
                prev_actual = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'actual'))
                prev_predicted = previous_data_index.get((row['province'], row['city'], row['year'], row['month'] - 1, 'predicted'))

            prev_row = prev_actual if prev_actual else prev_predicted

            # Ambil data tahun sebelumnya di bulan yang sama
            yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
            yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
            yoy_row = yoy_actual if yoy_actual else yoy_predicted

            # Hitung perubahan M-to-M dan Y-on-Y untuk semua metrik yang diperlukan
            for key in ['predicted_dbd_m', 'predicted_dbd_p']:
                # M-to-M change
                if prev_row and new_row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                    new_row[f'{key}_m_to_m_change'] = ((new_row[key] - prev_row[key]) / prev_row[key]) * 100
                else:
                    new_row[f'{key}_m_to_m_change'] = None
                
                # Y-on-Y change
                if yoy_row and new_row[key] is not None and yoy_row[key] is not None and yoy_row[key] != 0:
                    new_row[f'{key}_y_on_y_change'] = ((new_row[key] - yoy_row[key]) / yoy_row[key]) * 100
                else:
                    new_row[f'{key}_y_on_y_change'] = None
            
            # Tambahkan row yang telah diperbarui ke final_data
            final_data.append(new_row)
        
        return jsonify({"data": final_data, "success": True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.session.close()


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
                ),
                aggregated_climate AS (
                    SELECT
                        mkec.kd_prov,
                        mkec.kd_kab,
                        mkec.kd_kec,
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
                        mkec.kd_prov, mkec.kd_kab, mkec.kd_kec, climatemonthly.tahun, climatemonthly.bulan
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
                    AND cd.kd_kec = ac.kd_kec
                    AND cd.year = ac.tahun
                    AND cd.month = ac.bulan
                WHERE 
                    cd.kd_prov = :province
                    AND cd.kd_kab = :city
                    AND cd.kd_kec = :district
                
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
                select mk.*,mp.provinsi
                from masterkab mk
                left join masterprov mp 
                on mk.kd_prov = mp.kd_prov) hfi
            ON 
                dk.kd_kab = hfi.kd_kab
            GROUP BY 
                hfi.kd_prov, hfi.kd_kab, hfi.provinsi, hfi.kabkot, dk.tahun, dk.bulan, dk.status
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
            cd.kd_prov = :province and cd.kd_kab = :city
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
                for key in ['dbd_p', 'dbd_m','hujan_mean', 'hujan_max',
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
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['dbd_p', 'dbd_m','hujan_mean', 'hujan_max',
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
                for key in ['dbd_p', 'dbd_m','hujan_mean', 'hujan_max',
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
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['dbd_p', 'dbd_m','hujan_mean', 'hujan_max',
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
                cd.kd_prov = :province and cd.kd_kab = :city
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
                for key in ['lep_k', 'lep_m','hujan_mean', 'hujan_max',
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
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['city'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['lep_k', 'lep_m','hujan_mean', 'hujan_max',
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
            ,
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
                for key in ['lep_k', 'lep_m','hujan_mean', 'hujan_max',
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
                    if prev_row and row[key] is not None and prev_row[key] is not None and prev_row[key] != 0:
                        row[f'{key}_m_to_m_change'] = ((row[key] - prev_row[key]) / prev_row[key]) * 100
                    else:
                        row[f'{key}_m_to_m_change'] = None

                # Ambil data tahun sebelumnya di bulan yang sama
                yoy_actual = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'actual'))
                yoy_predicted = previous_data_index.get((row['province'], row['year'] - 1, row['month'], 'predicted'))
                yoy_row = yoy_actual if yoy_actual else yoy_predicted

                # Hitung perubahan Y-on-Y
                for key in ['lep_k', 'lep_m','hujan_mean', 'hujan_max',
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

@bp.route('/get-dbd-data', methods=['GET'])
@jwt_required()
def get_dbd_data():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        month_year = request.args.get('month_year', default=None, type=str)
        
        # Validasi parameter
        if not province:
            return jsonify({"error": "Parameter province wajib diisi"}), 400
        
        if not month_year or not re.match(r'^\d{4}-\d{2}$', month_year):
            return jsonify({"error": "Parameter month_year harus dalam format YYYY-MM"}), 400
        
        # Parse month_year
        year, month = map(int, month_year.split('-'))
        
        # Buat SQL query berdasarkan apakah city diisi atau tidak
        if city:
            # Query untuk kota/kabupaten tertentu
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req, 
                    :city as kd_kab_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
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
                    select mk.*,mp.provinsi
                    from masterkab mk
                    left join masterprov mp 
                    on mk.kd_prov = mp.kd_prov) hfi
                ON 
                    dk.kd_kab = hfi.kd_kab
                GROUP BY 
                    hfi.kd_prov, hfi.kd_kab, hfi.provinsi, hfi.kabkot, dk.tahun, dk.bulan, dk.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.kd_kab = rt.kd_kab_req
                   AND cd.year = rt.year_req
                   AND cd.month = rt.month_req
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.kd_kab = rt.kd_kab_req
                   AND cd.month = rt.month_req
                   AND cd.year = rt.year_req - 1
            ),
            current_data AS (
                SELECT 
                    cd.kd_prov,
                    cd.kd_kab,
                    cd.province,
                    cd.city,
                    cd.year,
                    cd.month,
                    cd.status,
                    cd.dbd_p,
                    cd.dbd_m,
                    'Data Aktual/Prediksi' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.kd_kab = rt.kd_kab_req
                    AND cd.year = rt.year_req
                    AND cd.month = rt.month_req
            ),
            previous_year_data AS (
                SELECT 
                    cd.kd_prov,
                    cd.kd_kab,
                    cd.province,
                    cd.city,
                    rt.year_req as year,
                    cd.month,
                    cd.status,
                    cd.dbd_p,
                    cd.dbd_m,
                    'Data Tahun Sebelumnya' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.kd_kab = rt.kd_kab_req
                    AND cd.month = rt.month_req
                    AND cd.year = rt.year_req - 1
            ),
            default_data AS (
                SELECT
                    rt.kd_prov_req as kd_prov,
                    rt.kd_kab_req as kd_kab,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = rt.kd_prov_req) as province,
                    (SELECT mk.kabkot FROM masterkab mk WHERE mk.kd_kab = rt.kd_kab_req) as city,
                    rt.year_req as year,
                    rt.month_req as month,
                    'no data' as status,
                    0 as dbd_p,
                    0 as dbd_m,
                    'Data Default (Semua 0)' as data_status
                FROM
                    requested_time rt
            )
            SELECT * FROM (
                SELECT * FROM current_data
                WHERE (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT * FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT * FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        else:
            # Query untuk agregasi provinsi (semua kabupaten)
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
                SELECT 
                    hfi.kd_prov,
                    hfi.provinsi AS province,
                    'Semua Kabupaten/Kota' AS city,
                    dk.tahun AS year,
                    dk.bulan AS month,
                    dk.status AS status,
                    SUM(dk."DBD_P") AS dbd_p,
                    SUM(dk."DBD_M") AS dbd_m
                FROM 
                    dbd dk
                JOIN 
                    (
                    select mk.*,mp.provinsi
                    from masterkab mk
                    left join masterprov mp 
                    on mk.kd_prov = mp.kd_prov) hfi
                ON 
                    dk.kd_kab = hfi.kd_kab
                WHERE
                    hfi.kd_prov = :province
                GROUP BY 
                    hfi.kd_prov, hfi.provinsi, dk.tahun, dk.bulan, dk.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.year = rt.year_req
                   AND cd.month = rt.month_req
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.month = rt.month_req
                   AND cd.year = rt.year_req - 1
            ),
            current_data AS (
                SELECT 
                    cd.kd_prov,
                    NULL as kd_kab,
                    cd.province,
                    cd.city,
                    cd.year,
                    cd.month,
                    cd.status,
                    cd.dbd_p,
                    cd.dbd_m,
                    'Data Aktual/Prediksi' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.year = rt.year_req
                    AND cd.month = rt.month_req
            ),
            previous_year_data AS (
                SELECT 
                    cd.kd_prov,
                    NULL as kd_kab,
                    cd.province,
                    cd.city,
                    rt.year_req as year,
                    cd.month,
                    cd.status,
                    cd.dbd_p,
                    cd.dbd_m,
                    'Data Tahun Sebelumnya' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.month = rt.month_req
                    AND cd.year = rt.year_req - 1
            ),
            default_data AS (
                SELECT
                    rt.kd_prov_req as kd_prov,
                    NULL as kd_kab,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = rt.kd_prov_req) as province,
                    'Semua Kabupaten/Kota' as city,
                    rt.year_req as year,
                    rt.month_req as month,
                    'no data' as status,
                    0 as dbd_p,
                    0 as dbd_m,
                    'Data Default (Semua 0)' as data_status
                FROM
                    requested_time rt
            )
            SELECT * FROM (
                SELECT * FROM current_data
                WHERE (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT * FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT * FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        
        # Eksekusi query dengan parameter
        params = {
            'province': province,
            'city': city,
            'year': year,
            'month': month
        }
        
        # Jalankan query dan dapatkan hasil sebagai dict
        result = db.session.execute(text(query), params).fetchone()
        
        if result:
            # Konversi hasil query menjadi dictionary
            # Pendekatan 1: Menggunakan _asdict() jika tersedia
            try:
                data = result._asdict()
            except AttributeError:
                # Pendekatan 2: Menggunakan dict(zip()) dengan kolom
                column_names = result._fields if hasattr(result, '_fields') else result.keys()
                data = dict(zip(column_names, result))
            
            # Tambahkan metadata dari parameter asli
            data['request_parameters'] = {
                'province': province,
                'city': city,
                'month_year': month_year
            }
            
            return jsonify(data)
        else:
            return jsonify({"error": "Data tidak ditemukan"}), 404
            
    except Exception as e:
        # Log error untuk debugging
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": f"Terjadi kesalahan: {str(e)}"}), 500
    finally:
        db.session.close()

@bp.route('/get-lepto-data', methods=['GET'])
@jwt_required()
def get_lepto_data():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        month_year = request.args.get('month_year', default=None, type=str)
        
        # Validasi parameter
        if not province:
            return jsonify({"error": "Parameter province wajib diisi"}), 400
        
        if not month_year or not re.match(r'^\d{4}-\d{2}$', month_year):
            return jsonify({"error": "Parameter month_year harus dalam format YYYY-MM"}), 400
        
        # Parse month_year
        year, month = map(int, month_year.split('-'))
        
        # Buat SQL query berdasarkan apakah city diisi atau tidak
        if city:
            # Query untuk kota/kabupaten tertentu
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req, 
                    :city as kd_kab_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
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
                    select mk.*,mp.provinsi
                    from masterkab mk
                    left join masterprov mp 
                    on mk.kd_prov = mp.kd_prov) hfi
                ON 
                    lk.kd_kab = hfi.kd_kab
                GROUP BY 
                    hfi.kd_prov, hfi.kd_kab, hfi.provinsi, hfi.kabkot, lk.tahun, lk.bulan, lk.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.kd_kab = rt.kd_kab_req
                   AND cd.year = rt.year_req
                   AND cd.month = rt.month_req
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.kd_kab = rt.kd_kab_req
                   AND cd.month = rt.month_req
                   AND cd.year = rt.year_req - 1
            ),
            current_data AS (
                SELECT 
                    cd.kd_prov,
                    cd.kd_kab,
                    cd.province,
                    cd.city,
                    cd.year,
                    cd.month,
                    cd.status,
                    cd.lep_k,
                    cd.lep_m,
                    'Data Aktual/Prediksi' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.kd_kab = rt.kd_kab_req
                    AND cd.year = rt.year_req
                    AND cd.month = rt.month_req
            ),
            previous_year_data AS (
                SELECT 
                    cd.kd_prov,
                    cd.kd_kab,
                    cd.province,
                    cd.city,
                    rt.year_req as year,
                    cd.month,
                    cd.status,
                    cd.lep_k,
                    cd.lep_m,
                    'Data Tahun Sebelumnya' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.kd_kab = rt.kd_kab_req
                    AND cd.month = rt.month_req
                    AND cd.year = rt.year_req - 1
            ),
            default_data AS (
                SELECT
                    rt.kd_prov_req as kd_prov,
                    rt.kd_kab_req as kd_kab,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = rt.kd_prov_req) as province,
                    (SELECT mk.kabkot FROM masterkab mk WHERE mk.kd_kab = rt.kd_kab_req) as city,
                    rt.year_req as year,
                    rt.month_req as month,
                    'no data' as status,
                    0 as lep_k,
                    0 as lep_m,
                    'Data Default (Semua 0)' as data_status
                FROM
                    requested_time rt
            )
            SELECT * FROM (
                SELECT * FROM current_data
                WHERE (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT * FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT * FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        else:
            # Query untuk agregasi provinsi (semua kabupaten)
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
                SELECT 
                    hfi.kd_prov,
                    hfi.provinsi AS province,
                    'Semua Kabupaten/Kota' AS city,
                    lk.tahun AS year,
                    lk.bulan AS month,
                    lk.status AS status,
                    SUM(lk."LEP_K") AS lep_k,
                    SUM(lk."LEP_M") AS lep_m
                FROM 
                    lepto lk
                JOIN 
                    (
                    select mk.*,mp.provinsi
                    from masterkab mk
                    left join masterprov mp 
                    on mk.kd_prov = mp.kd_prov) hfi
                ON 
                    lk.kd_kab = hfi.kd_kab
                WHERE
                    hfi.kd_prov = :province
                GROUP BY 
                    hfi.kd_prov, hfi.provinsi, lk.tahun, lk.bulan, lk.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.year = rt.year_req
                   AND cd.month = rt.month_req
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM combined_data cd
                JOIN requested_time rt
                ON cd.kd_prov = rt.kd_prov_req
                   AND cd.month = rt.month_req
                   AND cd.year = rt.year_req - 1
            ),
            current_data AS (
                SELECT 
                    cd.kd_prov,
                    NULL as kd_kab,
                    cd.province,
                    cd.city,
                    cd.year,
                    cd.month,
                    cd.status,
                    cd.lep_k,
                    cd.lep_m,
                    'Data Aktual/Prediksi' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.year = rt.year_req
                    AND cd.month = rt.month_req
            ),
            previous_year_data AS (
                SELECT 
                    cd.kd_prov,
                    NULL as kd_kab,
                    cd.province,
                    cd.city,
                    rt.year_req as year,
                    cd.month,
                    cd.status,
                    cd.lep_k,
                    cd.lep_m,
                    'Data Tahun Sebelumnya' as data_status
                FROM 
                    combined_data cd
                JOIN
                    requested_time rt
                ON
                    cd.kd_prov = rt.kd_prov_req
                    AND cd.month = rt.month_req
                    AND cd.year = rt.year_req - 1
            ),
            default_data AS (
                SELECT
                    rt.kd_prov_req as kd_prov,
                    NULL as kd_kab,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = rt.kd_prov_req) as province,
                    'Semua Kabupaten/Kota' as city,
                    rt.year_req as year,
                    rt.month_req as month,
                    'no data' as status,
                    0 as lep_k,
                    0 as lep_m,
                    'Data Default (Semua 0)' as data_status
                FROM
                    requested_time rt
            )
            SELECT * FROM (
                SELECT * FROM current_data
                WHERE (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT * FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT * FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        
        # Eksekusi query dengan parameter
        params = {
            'province': province,
            'city': city,
            'year': year,
            'month': month
        }
        
        # Jalankan query dan dapatkan hasil sebagai dict
        result = db.session.execute(text(query), params).fetchone()
        
        if result:
            # Konversi hasil query menjadi dictionary
            # Pendekatan 1: Menggunakan _asdict() jika tersedia
            try:
                data = result._asdict()
            except AttributeError:
                # Pendekatan 2: Menggunakan dict(zip()) dengan kolom
                column_names = result._fields if hasattr(result, '_fields') else result.keys()
                data = dict(zip(column_names, result))
            
            # Tambahkan metadata dari parameter asli
            data['request_parameters'] = {
                'province': province,
                'city': city,
                'month_year': month_year
            }
            
            return jsonify(data)
        else:
            return jsonify({"error": "Data tidak ditemukan"}), 404
            
    except Exception as e:
        # Log error untuk debugging
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": f"Terjadi kesalahan: {str(e)}"}), 500
    finally:
        db.session.close()
        
@bp.route('/get-malaria-data', methods=['GET'])
@jwt_required()
def get_malaria_data():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        city = request.args.get('city', default=None, type=str)
        district = request.args.get('district', default=None, type=str)
        month_year = request.args.get('month_year', default=None, type=str)
        
        # Validasi parameter
        if not province:
            return jsonify({"error": "Parameter province wajib diisi"}), 400
        
        if not month_year or not re.match(r'^\d{4}-\d{2}$', month_year):
            return jsonify({"error": "Parameter month_year harus dalam format YYYY-MM"}), 400
        
        # Parse month_year
        year, month = map(int, month_year.split('-'))
        
        # Buat SQL query berdasarkan level detail yang diminta
        if district and city:
            # Query untuk kecamatan tertentu
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req, 
                    :city as kd_kab_req,
                    :district as kd_kec_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
                SELECT 
                    hf.kd_prov, hf.kd_kab, hf.kd_kec,
                    mp.provinsi AS province,
                    mk.kabkot AS city,
                    mkec.kecamatan AS district,
                    mhfm.tahun AS year,
                    mhfm.bulan AS month,
                    mhfm.status AS status,
                    SUM(COALESCE(mhfm.tot_pos, 0)) AS tot_pos,
                    SUM(COALESCE(mhfm.konfirmasi_lab_mikroskop, 0)) as konfirmasi_lab_mikroskop,
                    SUM(COALESCE(mhfm.konfirmasi_lab_rdt, 0)) as konfirmasi_lab_rdt,
                    SUM(COALESCE(mhfm.konfirmasi_lab_pcr, 0)) as konfirmasi_lab_pcr,
                    SUM(COALESCE(mhfm.pos_0_4, 0)) as pos_0_4,
                    SUM(COALESCE(mhfm.pos_5_14, 0)) as pos_5_14,
                    SUM(COALESCE(mhfm.pos_15_64, 0)) as pos_15_64,
                    SUM(COALESCE(mhfm.pos_diatas_64, 0)) as pos_diatas_64,
                    SUM(COALESCE(mhfm.hamil_pos, 0)) as hamil_pos,
                    SUM(COALESCE(mhfm.kematian_malaria, 0)) as kematian_malaria,
                    SUM(COALESCE(mhfm.obat_standar, 0)) as obat_standar,
                    SUM(COALESCE(mhfm.obat_nonprogram, 0)) as obat_nonprogram,
                    SUM(COALESCE(mhfm.obat_primaquin, 0)) as obat_primaquin,
                    SUM(COALESCE(mhfm.p_pf, 0)) as p_pf,
                    SUM(COALESCE(mhfm.p_pv, 0)) as p_pv,
                    SUM(COALESCE(mhfm.p_po, 0)) as p_po,
                    SUM(COALESCE(mhfm.p_pm, 0)) as p_pm,
                    SUM(COALESCE(mhfm.p_pk, 0)) as p_pk,
                    SUM(COALESCE(mhfm.p_mix, 0)) as p_mix,
                    SUM(COALESCE(mhfm.p_suspek_pk, 0)) as p_suspek_pk,
                    SUM(COALESCE(mhfm.p_others, 0)) as p_others,
                    SUM(COALESCE(mhfm.penularan_indigenus, 0)) as penularan_indigenus,
                    SUM(COALESCE(mhfm.penularan_impor, 0)) as penularan_impor,
                    SUM(COALESCE(mhfm.penularan_induced, 0)) as penularan_induced,
                    SUM(COALESCE(mhfm.relaps, 0)) as relaps
                FROM 
                    healthfacility hf
                LEFT JOIN 
                    masterkec mkec ON hf.kd_kec = mkec.kd_kec
                LEFT JOIN 
                    masterkab mk ON hf.kd_kab = mk.kd_kab
                LEFT JOIN 
                    masterprov mp ON hf.kd_prov = mp.kd_prov
                LEFT JOIN 
                    (
                    SELECT * FROM malariamonthly
                    WHERE tahun = :year AND bulan = :month
                    ) mhfm ON hf.id_faskes = mhfm.id_faskes
                WHERE
                    hf.kd_prov = :province
                    AND hf.kd_kab = :city
                    AND hf.kd_kec = :district
                GROUP BY 
                    hf.kd_prov, hf.kd_kab, hf.kd_kec, mp.provinsi, mk.kabkot, mkec.kecamatan, 
                    mhfm.tahun, mhfm.bulan, mhfm.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data
                WHERE year IS NOT NULL
            ),
            previous_year_data AS (
                SELECT 
                    hf.kd_prov, hf.kd_kab, hf.kd_kec,
                    mp.provinsi AS province,
                    mk.kabkot AS city,
                    mkec.kecamatan AS district,
                    :year as year,
                    :month as month,
                    mhfm.status AS status,
                    SUM(COALESCE(mhfm.tot_pos, 0)) AS tot_pos,
                    SUM(COALESCE(mhfm.konfirmasi_lab_mikroskop, 0)) as konfirmasi_lab_mikroskop,
                    SUM(COALESCE(mhfm.konfirmasi_lab_rdt, 0)) as konfirmasi_lab_rdt,
                    SUM(COALESCE(mhfm.konfirmasi_lab_pcr, 0)) as konfirmasi_lab_pcr,
                    SUM(COALESCE(mhfm.pos_0_4, 0)) as pos_0_4,
                    SUM(COALESCE(mhfm.pos_5_14, 0)) as pos_5_14,
                    SUM(COALESCE(mhfm.pos_15_64, 0)) as pos_15_64,
                    SUM(COALESCE(mhfm.pos_diatas_64, 0)) as pos_diatas_64,
                    SUM(COALESCE(mhfm.hamil_pos, 0)) as hamil_pos,
                    SUM(COALESCE(mhfm.kematian_malaria, 0)) as kematian_malaria,
                    SUM(COALESCE(mhfm.obat_standar, 0)) as obat_standar,
                    SUM(COALESCE(mhfm.obat_nonprogram, 0)) as obat_nonprogram,
                    SUM(COALESCE(mhfm.obat_primaquin, 0)) as obat_primaquin,
                    SUM(COALESCE(mhfm.p_pf, 0)) as p_pf,
                    SUM(COALESCE(mhfm.p_pv, 0)) as p_pv,
                    SUM(COALESCE(mhfm.p_po, 0)) as p_po,
                    SUM(COALESCE(mhfm.p_pm, 0)) as p_pm,
                    SUM(COALESCE(mhfm.p_pk, 0)) as p_pk,
                    SUM(COALESCE(mhfm.p_mix, 0)) as p_mix,
                    SUM(COALESCE(mhfm.p_suspek_pk, 0)) as p_suspek_pk,
                    SUM(COALESCE(mhfm.p_others, 0)) as p_others,
                    SUM(COALESCE(mhfm.penularan_indigenus, 0)) as penularan_indigenus,
                    SUM(COALESCE(mhfm.penularan_impor, 0)) as penularan_impor,
                    SUM(COALESCE(mhfm.penularan_induced, 0)) as penularan_induced,
                    SUM(COALESCE(mhfm.relaps, 0)) as relaps
                FROM 
                    healthfacility hf
                LEFT JOIN 
                    masterkec mkec ON hf.kd_kec = mkec.kd_kec
                LEFT JOIN 
                    masterkab mk ON hf.kd_kab = mk.kd_kab
                LEFT JOIN 
                    masterprov mp ON hf.kd_prov = mp.kd_prov
                LEFT JOIN 
                    (
                    SELECT * FROM malariamonthly
                    WHERE tahun = :year - 1 AND bulan = :month
                    ) mhfm ON hf.id_faskes = mhfm.id_faskes
                WHERE
                    hf.kd_prov = :province
                    AND hf.kd_kab = :city
                    AND hf.kd_kec = :district
                GROUP BY 
                    hf.kd_prov, hf.kd_kab, hf.kd_kec, mp.provinsi, mk.kabkot, mkec.kecamatan, 
                    mhfm.status
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM previous_year_data
                WHERE status IS NOT NULL
            ),
            default_data AS (
                SELECT
                    :province as kd_prov,
                    :city as kd_kab,
                    :district as kd_kec,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = :province) as province,
                    (SELECT mk.kabkot FROM masterkab mk WHERE mk.kd_kab = :city) as city,
                    (SELECT mkec.kecamatan FROM masterkec mkec WHERE mkec.kd_kec = :district) as district,
                    :year as year,
                    :month as month,
                    'no data' as status,
                    0 as tot_pos,
                    0 as konfirmasi_lab_mikroskop,
                    0 as konfirmasi_lab_rdt,
                    0 as konfirmasi_lab_pcr,
                    0 as pos_0_4,
                    0 as pos_5_14,
                    0 as pos_15_64,
                    0 as pos_diatas_64,
                    0 as hamil_pos,
                    0 as kematian_malaria,
                    0 as obat_standar,
                    0 as obat_nonprogram,
                    0 as obat_primaquin,
                    0 as p_pf,
                    0 as p_pv,
                    0 as p_po,
                    0 as p_pm,
                    0 as p_pk,
                    0 as p_mix,
                    0 as p_suspek_pk,
                    0 as p_others,
                    0 as penularan_indigenus,
                    0 as penularan_impor,
                    0 as penularan_induced,
                    0 as relaps
            )
		    SELECT * FROM (
                SELECT *, 'Data Aktual/Prediksi' as data_status FROM combined_data
                WHERE year IS NOT NULL AND (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT *, 'Data Tahun Sebelumnya' as data_status FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT *, 'Data Default (Semua 0)' as data_status FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        elif city and not district:
            # Query untuk kota/kabupaten tanpa kecamatan tertentu
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req, 
                    :city as kd_kab_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
                SELECT 
                    hf.kd_prov, hf.kd_kab,
                    mp.provinsi AS province,
                    mk.kabkot AS city,
                    NULL as kd_kec,
                    'Semua Kecamatan' AS district,
                    mhfm.tahun AS year,
                    mhfm.bulan AS month,
                    mhfm.status AS status,
                    SUM(COALESCE(mhfm.tot_pos, 0)) AS tot_pos,
                    SUM(COALESCE(mhfm.konfirmasi_lab_mikroskop, 0)) as konfirmasi_lab_mikroskop,
                    SUM(COALESCE(mhfm.konfirmasi_lab_rdt, 0)) as konfirmasi_lab_rdt,
                    SUM(COALESCE(mhfm.konfirmasi_lab_pcr, 0)) as konfirmasi_lab_pcr,
                    SUM(COALESCE(mhfm.pos_0_4, 0)) as pos_0_4,
                    SUM(COALESCE(mhfm.pos_5_14, 0)) as pos_5_14,
                    SUM(COALESCE(mhfm.pos_15_64, 0)) as pos_15_64,
                    SUM(COALESCE(mhfm.pos_diatas_64, 0)) as pos_diatas_64,
                    SUM(COALESCE(mhfm.hamil_pos, 0)) as hamil_pos,
                    SUM(COALESCE(mhfm.kematian_malaria, 0)) as kematian_malaria,
                    SUM(COALESCE(mhfm.obat_standar, 0)) as obat_standar,
                    SUM(COALESCE(mhfm.obat_nonprogram, 0)) as obat_nonprogram,
                    SUM(COALESCE(mhfm.obat_primaquin, 0)) as obat_primaquin,
                    SUM(COALESCE(mhfm.p_pf, 0)) as p_pf,
                    SUM(COALESCE(mhfm.p_pv, 0)) as p_pv,
                    SUM(COALESCE(mhfm.p_po, 0)) as p_po,
                    SUM(COALESCE(mhfm.p_pm, 0)) as p_pm,
                    SUM(COALESCE(mhfm.p_pk, 0)) as p_pk,
                    SUM(COALESCE(mhfm.p_mix, 0)) as p_mix,
                    SUM(COALESCE(mhfm.p_suspek_pk, 0)) as p_suspek_pk,
                    SUM(COALESCE(mhfm.p_others, 0)) as p_others,
                    SUM(COALESCE(mhfm.penularan_indigenus, 0)) as penularan_indigenus,
                    SUM(COALESCE(mhfm.penularan_impor, 0)) as penularan_impor,
                    SUM(COALESCE(mhfm.penularan_induced, 0)) as penularan_induced,
                    SUM(COALESCE(mhfm.relaps, 0)) as relaps
                FROM 
                    healthfacility hf
                LEFT JOIN 
                    masterkec mkec ON hf.kd_kec = mkec.kd_kec
                LEFT JOIN 
                    masterkab mk ON hf.kd_kab = mk.kd_kab
                LEFT JOIN 
                    masterprov mp ON hf.kd_prov = mp.kd_prov
                LEFT JOIN 
                    (
                    SELECT * FROM malariamonthly
                    WHERE tahun = :year AND bulan = :month
                    ) mhfm ON hf.id_faskes = mhfm.id_faskes
                WHERE
                    hf.kd_prov = :province
                    AND hf.kd_kab = :city
                GROUP BY 
                    hf.kd_prov, hf.kd_kab, mp.provinsi, mk.kabkot, 
                    mhfm.tahun, mhfm.bulan, mhfm.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data
                WHERE year IS NOT NULL
            ),
            previous_year_data AS (
                SELECT 
                    hf.kd_prov, hf.kd_kab,
                    mp.provinsi AS province,
                    mk.kabkot AS city,
                    NULL as kd_kec,
                    'Semua Kecamatan' AS district,
                    :year as year,
                    :month as month,
                    mhfm.status AS status,
                    SUM(COALESCE(mhfm.tot_pos, 0)) AS tot_pos,
                    SUM(COALESCE(mhfm.konfirmasi_lab_mikroskop, 0)) as konfirmasi_lab_mikroskop,
                    SUM(COALESCE(mhfm.konfirmasi_lab_rdt, 0)) as konfirmasi_lab_rdt,
                    SUM(COALESCE(mhfm.konfirmasi_lab_pcr, 0)) as konfirmasi_lab_pcr,
                    SUM(COALESCE(mhfm.pos_0_4, 0)) as pos_0_4,
                    SUM(COALESCE(mhfm.pos_5_14, 0)) as pos_5_14,
                    SUM(COALESCE(mhfm.pos_15_64, 0)) as pos_15_64,
                    SUM(COALESCE(mhfm.pos_diatas_64, 0)) as pos_diatas_64,
                    SUM(COALESCE(mhfm.hamil_pos, 0)) as hamil_pos,
                    SUM(COALESCE(mhfm.kematian_malaria, 0)) as kematian_malaria,
                    SUM(COALESCE(mhfm.obat_standar, 0)) as obat_standar,
                    SUM(COALESCE(mhfm.obat_nonprogram, 0)) as obat_nonprogram,
                    SUM(COALESCE(mhfm.obat_primaquin, 0)) as obat_primaquin,
                    SUM(COALESCE(mhfm.p_pf, 0)) as p_pf,
                    SUM(COALESCE(mhfm.p_pv, 0)) as p_pv,
                    SUM(COALESCE(mhfm.p_po, 0)) as p_po,
                    SUM(COALESCE(mhfm.p_pm, 0)) as p_pm,
                    SUM(COALESCE(mhfm.p_pk, 0)) as p_pk,
                    SUM(COALESCE(mhfm.p_mix, 0)) as p_mix,
                    SUM(COALESCE(mhfm.p_suspek_pk, 0)) as p_suspek_pk,
                    SUM(COALESCE(mhfm.p_others, 0)) as p_others,
                    SUM(COALESCE(mhfm.penularan_indigenus, 0)) as penularan_indigenus,
                    SUM(COALESCE(mhfm.penularan_impor, 0)) as penularan_impor,
                    SUM(COALESCE(mhfm.penularan_induced, 0)) as penularan_induced,
                    SUM(COALESCE(mhfm.relaps, 0)) as relaps
                FROM 
                    healthfacility hf
                LEFT JOIN 
                    masterkec mkec ON hf.kd_kec = mkec.kd_kec
                LEFT JOIN 
                    masterkab mk ON hf.kd_kab = mk.kd_kab
                LEFT JOIN 
                    masterprov mp ON hf.kd_prov = mp.kd_prov
                LEFT JOIN 
                    (
                    SELECT * FROM malariamonthly
                    WHERE tahun = :year - 1 AND bulan = :month
                    ) mhfm ON hf.id_faskes = mhfm.id_faskes
                WHERE
                    hf.kd_prov = :province
                    AND hf.kd_kab = :city
                GROUP BY 
                    hf.kd_prov, hf.kd_kab, mp.provinsi, mk.kabkot, 
                    mhfm.status
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM previous_year_data
                WHERE status IS NOT NULL
            ),
            default_data AS (
                SELECT
                    :province as kd_prov,
                    :city as kd_kab,
                    NULL as kd_kec,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = :province) as province,
                    (SELECT mk.kabkot FROM masterkab mk WHERE mk.kd_kab = :city) as city,
                    'Semua Kecamatan' as district,
                    :year as year,
                    :month as month,
                    'no data' as status,
                    0 as tot_pos,
                    0 as konfirmasi_lab_mikroskop,
                    0 as konfirmasi_lab_rdt,
                    0 as konfirmasi_lab_pcr,
                    0 as pos_0_4,
                    0 as pos_5_14,
                    0 as pos_15_64,
                    0 as pos_diatas_64,
                    0 as hamil_pos,
                    0 as kematian_malaria,
                    0 as obat_standar,
                    0 as obat_nonprogram,
                    0 as obat_primaquin,
                    0 as p_pf,
                    0 as p_pv,
                    0 as p_po,
                    0 as p_pm,
                    0 as p_pk,
                    0 as p_mix,
                    0 as p_suspek_pk,
                    0 as p_others,
                    0 as penularan_indigenus,
                    0 as penularan_impor,
                    0 as penularan_induced,
                    0 as relaps
            )

		    SELECT * FROM (
                SELECT *, 'Data Aktual/Prediksi' as data_status FROM combined_data
                WHERE year IS NOT NULL AND (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT *, 'Data Tahun Sebelumnya' as data_status FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT *, 'Data Default (Semua 0)' as data_status FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        else:
            # Query untuk agregasi provinsi (semua kabupaten)
            query = """
            WITH requested_time AS (
                SELECT 
                    :province as kd_prov_req,
                    :year as year_req,
                    :month as month_req
            ),
            combined_data AS (
                SELECT 
                    hf.kd_prov,
                    mp.provinsi AS province,
                    NULL as kd_kab,
                    NULL as kd_kec,
                    'Semua Kabupaten/Kota' AS city,
                    'Semua Kecamatan' AS district,
                    mhfm.tahun AS year,
                    mhfm.bulan AS month,
                    mhfm.status AS status,
                    SUM(COALESCE(mhfm.tot_pos, 0)) AS tot_pos,
                    SUM(COALESCE(mhfm.konfirmasi_lab_mikroskop, 0)) as konfirmasi_lab_mikroskop,
                    SUM(COALESCE(mhfm.konfirmasi_lab_rdt, 0)) as konfirmasi_lab_rdt,
                    SUM(COALESCE(mhfm.konfirmasi_lab_pcr, 0)) as konfirmasi_lab_pcr,
                    SUM(COALESCE(mhfm.pos_0_4, 0)) as pos_0_4,
                    SUM(COALESCE(mhfm.pos_5_14, 0)) as pos_5_14,
                    SUM(COALESCE(mhfm.pos_15_64, 0)) as pos_15_64,
                    SUM(COALESCE(mhfm.pos_diatas_64, 0)) as pos_diatas_64,
                    SUM(COALESCE(mhfm.hamil_pos, 0)) as hamil_pos,
                    SUM(COALESCE(mhfm.kematian_malaria, 0)) as kematian_malaria,
                    SUM(COALESCE(mhfm.obat_standar, 0)) as obat_standar,
                    SUM(COALESCE(mhfm.obat_nonprogram, 0)) as obat_nonprogram,
                    SUM(COALESCE(mhfm.obat_primaquin, 0)) as obat_primaquin,
                    SUM(COALESCE(mhfm.p_pf, 0)) as p_pf,
                    SUM(COALESCE(mhfm.p_pv, 0)) as p_pv,
                    SUM(COALESCE(mhfm.p_po, 0)) as p_po,
                    SUM(COALESCE(mhfm.p_pm, 0)) as p_pm,
                    SUM(COALESCE(mhfm.p_pk, 0)) as p_pk,
                    SUM(COALESCE(mhfm.p_mix, 0)) as p_mix,
                    SUM(COALESCE(mhfm.p_suspek_pk, 0)) as p_suspek_pk,
                    SUM(COALESCE(mhfm.p_others, 0)) as p_others,
                    SUM(COALESCE(mhfm.penularan_indigenus, 0)) as penularan_indigenus,
                    SUM(COALESCE(mhfm.penularan_impor, 0)) as penularan_impor,
                    SUM(COALESCE(mhfm.penularan_induced, 0)) as penularan_induced,
                    SUM(COALESCE(mhfm.relaps, 0)) as relaps
                FROM 
                    healthfacility hf
                LEFT JOIN 
                    masterkec mkec ON hf.kd_kec = mkec.kd_kec
                LEFT JOIN 
                    masterkab mk ON hf.kd_kab = mk.kd_kab
                LEFT JOIN 
                    masterprov mp ON hf.kd_prov = mp.kd_prov
                LEFT JOIN 
                    (
                    SELECT * FROM malariamonthly
                    WHERE tahun = :year AND bulan = :month
                    ) mhfm ON hf.id_faskes = mhfm.id_faskes
                WHERE
                    hf.kd_prov = :province
                GROUP BY 
                    hf.kd_prov, mp.provinsi, 
                    mhfm.tahun, mhfm.bulan, mhfm.status
            ),
            check_current_data AS (
                SELECT COUNT(*) as count
                FROM combined_data
                WHERE year IS NOT NULL
            ),
            previous_year_data AS (
                SELECT 
                    hf.kd_prov,
                    mp.provinsi AS province,
                    NULL as kd_kab,
                    NULL as kd_kec,
                    'Semua Kabupaten/Kota' AS city,
                    'Semua Kecamatan' AS district,
                    :year as year,
                    :month as month,
                    mhfm.status AS status,
                    SUM(COALESCE(mhfm.tot_pos, 0)) AS tot_pos,
                    SUM(COALESCE(mhfm.konfirmasi_lab_mikroskop, 0)) as konfirmasi_lab_mikroskop,
                    SUM(COALESCE(mhfm.konfirmasi_lab_rdt, 0)) as konfirmasi_lab_rdt,
                    SUM(COALESCE(mhfm.konfirmasi_lab_pcr, 0)) as konfirmasi_lab_pcr,
                    SUM(COALESCE(mhfm.pos_0_4, 0)) as pos_0_4,
                    SUM(COALESCE(mhfm.pos_5_14, 0)) as pos_5_14,
                    SUM(COALESCE(mhfm.pos_15_64, 0)) as pos_15_64,
                    SUM(COALESCE(mhfm.pos_diatas_64, 0)) as pos_diatas_64,
                    SUM(COALESCE(mhfm.hamil_pos, 0)) as hamil_pos,
                    SUM(COALESCE(mhfm.kematian_malaria, 0)) as kematian_malaria,
                    SUM(COALESCE(mhfm.obat_standar, 0)) as obat_standar,
                    SUM(COALESCE(mhfm.obat_nonprogram, 0)) as obat_nonprogram,
                    SUM(COALESCE(mhfm.obat_primaquin, 0)) as obat_primaquin,
                    SUM(COALESCE(mhfm.p_pf, 0)) as p_pf,
                    SUM(COALESCE(mhfm.p_pv, 0)) as p_pv,
                    SUM(COALESCE(mhfm.p_po, 0)) as p_po,
                    SUM(COALESCE(mhfm.p_pm, 0)) as p_pm,
                    SUM(COALESCE(mhfm.p_pk, 0)) as p_pk,
                    SUM(COALESCE(mhfm.p_mix, 0)) as p_mix,
                    SUM(COALESCE(mhfm.p_suspek_pk, 0)) as p_suspek_pk,
                    SUM(COALESCE(mhfm.p_others, 0)) as p_others,
                    SUM(COALESCE(mhfm.penularan_indigenus, 0)) as penularan_indigenus,
                    SUM(COALESCE(mhfm.penularan_impor, 0)) as penularan_impor,
                    SUM(COALESCE(mhfm.penularan_induced, 0)) as penularan_induced,
                    SUM(COALESCE(mhfm.relaps, 0)) as relaps
                FROM 
                    healthfacility hf
                LEFT JOIN 
                    masterkec mkec ON hf.kd_kec = mkec.kd_kec
                LEFT JOIN 
                    masterkab mk ON hf.kd_kab = mk.kd_kab
                LEFT JOIN 
                    masterprov mp ON hf.kd_prov = mp.kd_prov
                LEFT JOIN 
                    (
                    SELECT * FROM malariamonthly
                    WHERE tahun = :year - 1 AND bulan = :month
                    ) mhfm ON hf.id_faskes = mhfm.id_faskes
                WHERE
                    hf.kd_prov = :province
                GROUP BY 
                    hf.kd_prov, mp.provinsi, 
                    mhfm.status
            ),
            check_previous_year_data AS (
                SELECT COUNT(*) as count
                FROM previous_year_data
                WHERE status IS NOT NULL
            ),
            default_data AS (
                SELECT
                    :province as kd_prov,
                    NULL as kd_kab,
                    NULL as kd_kec,
                    (SELECT mp.provinsi FROM masterprov mp WHERE mp.kd_prov = :province) as province,
                    'Semua Kabupaten/Kota' as city,
                    'Semua Kecamatan' as district,
                    :year as year,
                    :month as month,
                    'no data' as status,
                    0 as tot_pos,
                    0 as konfirmasi_lab_mikroskop,
                    0 as konfirmasi_lab_rdt,
                    0 as konfirmasi_lab_pcr,
                    0 as pos_0_4,
                    0 as pos_5_14,
                    0 as pos_15_64,
                    0 as pos_diatas_64,
                    0 as hamil_pos,
                    0 as kematian_malaria,
                    0 as obat_standar,
                    0 as obat_nonprogram,
                    0 as obat_primaquin,
                    0 as p_pf,
                    0 as p_pv,
                    0 as p_po,
                    0 as p_pm,
                    0 as p_pk,
                    0 as p_mix,
                    0 as p_suspek_pk,
                    0 as p_others,
                    0 as penularan_indigenus,
                    0 as penularan_impor,
                    0 as penularan_induced,
                    0 as relaps
            )
		    SELECT * FROM (
                SELECT *, 'Data Aktual/Prediksi' as data_status FROM combined_data
                WHERE year IS NOT NULL AND (SELECT count FROM check_current_data) > 0
                
                UNION ALL
                
                SELECT *, 'Data Tahun Sebelumnya' as data_status FROM previous_year_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) > 0
                
                UNION ALL
                
                SELECT *, 'Data Default (Semua 0)' as data_status FROM default_data
                WHERE (SELECT count FROM check_current_data) = 0 
                AND (SELECT count FROM check_previous_year_data) = 0
            ) final_data
            LIMIT 1;
            """
        
        # Eksekusi query dengan parameter
        params = {
            'province': province,
            'city': city,
            'district': district,
            'year': year,
            'month': month
        }
        
        # Jalankan query dan dapatkan hasil sebagai dict
        result = db.session.execute(text(query), params).fetchone()
        
        if result:
            # Konversi hasil query menjadi dictionary
            # Pendekatan 1: Menggunakan _asdict() jika tersedia
            try:
                data = result._asdict()
            except AttributeError:
                # Pendekatan 2: Menggunakan dict(zip()) dengan kolom
                column_names = result._fields if hasattr(result, '_fields') else result.keys()
                data = dict(zip(column_names, result))
            
            # Tambahkan metadata dari parameter asli
            data['request_parameters'] = {
                'province': province,
                'city': city,
                'district': district,
                'month_year': month_year
            }
            
            return jsonify(data)
        else:
            return jsonify({"error": "Data tidak ditemukan"}), 404
            
    except Exception as e:
        # Log error untuk debugging
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": f"Terjadi kesalahan: {str(e)}"}), 500
    finally:
        db.session.close()