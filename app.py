from datetime import timedelta
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
import uuid
import os
import requests
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import os
import urllib.parse
# Memuat variabel dari file .env
load_dotenv()

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})



# Konfigurasi database
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = urllib.parse.quote_plus(os.getenv('DB_PASSWORD'))
DB_HOST = os.getenv('DB_HOST')

app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 's3creeer2312k3wewad!'

db = SQLAlchemy(app)
jwt = JWTManager(app)

# Swagger UI configuration
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.yaml'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "DiplomatAI"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Model pengguna
class MalariaHealthFacilityMonthly(db.Model):
    __tablename__ = 'malaria_health_facility_monthly'
    id_mhfm = db.Column(db.Integer, primary_key=True, autoincrement=True)
    id_faskes = db.Column(db.Integer, db.ForeignKey('health_facility_id.id_faskes'), nullable=False)
    bulan = db.Column(db.Integer, nullable=False)
    tahun = db.Column(db.Integer, nullable=False)
    konfirmasi_lab_mikroskop = db.Column(db.Integer)
    konfirmasi_lab_rdt = db.Column(db.Integer)
    konfirmasi_lab_pcr = db.Column(db.Integer)
    total_konfirmasi_lab = db.Column(db.Integer)
    pos_0_1_m = db.Column(db.Integer)
    pos_0_1_f = db.Column(db.Integer)
    pos_1_4_m = db.Column(db.Integer)
    pos_1_4_f = db.Column(db.Integer)
    pos_5_9_m = db.Column(db.Integer)
    pos_5_9_f = db.Column(db.Integer)
    pos_10_14_m = db.Column(db.Integer)
    pos_10_14_f = db.Column(db.Integer)
    pos_15_64_m = db.Column(db.Integer)
    pos_15_64_f = db.Column(db.Integer)
    pos_diatas_64_m = db.Column(db.Integer)
    pos_diatas_64_f = db.Column(db.Integer)
    tot_pos_m = db.Column(db.Integer)
    tot_pos_f = db.Column(db.Integer)
    tot_pos = db.Column(db.Integer)
    kematian_malaria = db.Column(db.Integer)
    hamil_pos = db.Column(db.Integer)
    p_pf = db.Column(db.Integer)
    p_pv = db.Column(db.Integer)
    p_po = db.Column(db.Integer)
    p_pm = db.Column(db.Integer)
    p_pk = db.Column(db.Integer)
    p_mix = db.Column(db.Integer)
    p_suspek_pk = db.Column(db.Integer)
    obat_standar = db.Column(db.Integer)
    obat_nonprogram = db.Column(db.Integer)
    obat_primaquin = db.Column(db.Integer)
    kasus_pe = db.Column(db.Integer)
    penularan_indigenus = db.Column(db.Integer)
    penularan_impor = db.Column(db.Integer)
    penularan_induced = db.Column(db.Integer)
    relaps = db.Column(db.Integer)
    indikator_pengobatan_standar = db.Column(db.Integer)
    indikator_primaquin = db.Column(db.Integer)
    indikator_kasus_pe = db.Column(db.Integer)
    status = db.Column(db.String(10), default="actual")

class HealthFacilityId(db.Model):
    __tablename__ = 'health_facility_id'

    id_faskes = db.Column(db.Integer, primary_key=True)
    provinsi = db.Column(db.String(45), nullable=False)
    kabupaten = db.Column(db.String(45), nullable=False)
    kecamatan = db.Column(db.String(45), nullable=False)
    owner = db.Column(db.String(100))
    tipe_faskes = db.Column(db.String(100))
    nama_faskes = db.Column(db.String(100))
    address = db.Column(db.String(255))
    url = db.Column(db.Text)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    mhfm = db.relationship('MalariaHealthFacilityMonthly', backref='HealthFacilityId', uselist=False)

class User(db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    full_name = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, default=db.func.current_timestamp(), onupdate=db.func.current_timestamp())
    phone_number = db.Column(db.String(15))
    address_1 = db.Column(db.String(255))
    address_2 = db.Column(db.String(255))
    access_level = db.Column(db.String(50), default='user')
    def to_dict(self):
        return {
            "id": self.id,
            "email": self.email,
            "full_name": self.full_name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "phone_number": self.phone_number,
            "address_1": self.address_1,
            "address_2": self.address_2,
            "access_level": self.access_level
        }
    

@app.route('/signup', methods=['POST'])
def signup():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    full_name = data.get('full_name')
    phone_number = data.get('phone_number')
    address_1 = data.get('address_1')
    address_2 = data.get('address_2')
    access_level = data.get('access_level')
    

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    # Cek apakah user sudah ada
    existing_user = User.query.filter_by(email=email).first()
    if existing_user:
        return jsonify({"error": "User already exists"}), 400

    # Hash password
    hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
    
    # Buat user baru
    new_user = User(
        email=email,
        password=hashed_password,
        full_name=full_name,
        phone_number=phone_number,
        address_1=address_1,
        address_2=address_2,
        access_level=access_level,
    )
    db.session.add(new_user)
    db.session.flush()  # Pastikan ID user tersedia sebelum menyimpan relasi

    db.session.commit()

    # Buat access_token
    access_token = create_access_token(identity={'email': new_user.email}, expires_delta=timedelta(hours=1))

    return jsonify({"message": "Login successful", "access_token": access_token,"user":new_user.to_dict(),"success":True}), 201

@app.route('/signin', methods=['POST'])
def signin():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    user = User.query.filter_by(email=email).first()
    if not user or not check_password_hash(user.password, password):
        return jsonify({"error": "Invalid credentials"}), 401
    print(user)
    # Buat token JWT
    access_token = create_access_token(identity={'username': user.email, 'access_level': user.access_level}, expires_delta=timedelta(hours=1))
    return jsonify({"message": "Login successful", "access_token": access_token,"user":user.to_dict(),"success":True}), 200

# Middleware untuk memeriksa akses berdasarkan role
def role_required(required_role):
    def decorator(func):
        @jwt_required()
        def wrapper(*args, **kwargs):
            identity = get_jwt_identity()
            if identity.get('role') != required_role:
                return jsonify({"error": "Access denied"}), 403
            return func(*args, **kwargs)
        return wrapper
    return decorator
@app.route('/get-provinces', methods=['GET'])
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
        print(query)
        result = db.session.execute(query).fetchall()
        provinces = [row.province for row in result]
        
        return jsonify({"data": provinces,"success":True}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get-raw-data', methods=['GET'])
@jwt_required()
def get_raw_data():
    try:
        # Ambil parameter dari request
        province = request.args.get('province', default=None, type=str)
        if province == 'TOTAL':
            province = None
        print('parameter', province)

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
                mhfm.pos_0_1_m,
                mhfm.pos_0_1_f,
                mhfm.pos_1_4_m,
                mhfm.pos_1_4_f,
                mhfm.pos_5_9_m,
                mhfm.pos_5_9_f,
                mhfm.pos_10_14_m,
                mhfm.pos_10_14_f,
                mhfm.pos_15_64_m,
                mhfm.pos_15_64_f,
                mhfm.pos_diatas_64_m,
                mhfm.pos_diatas_64_f,
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
        # Langkah 2: Filter data untuk tahun dan bulan terakhir
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

@app.route('/get-aggregate-data', methods=['GET'])
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
                    SUM(mhfm.pos_0_1_m)+SUM(mhfm.pos_0_1_f)+SUM(mhfm.pos_1_4_m)+SUM(mhfm.pos_1_4_f) as pos_0_4,
                    SUM(mhfm.pos_5_9_m)+SUM(mhfm.pos_5_9_f)+SUM(mhfm.pos_10_14_m)+SUM(mhfm.pos_10_14_f) as pos_5_14,
                    SUM(mhfm.pos_15_64_m)+SUM(mhfm.pos_15_64_f) as pos_15_64,
                    SUM(mhfm.pos_diatas_64_m)+SUM(mhfm.pos_diatas_64_f) as pos_diatas_64,
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
                    SUM(mhfm.pos_0_1_m)+SUM(mhfm.pos_0_1_f)+SUM(mhfm.pos_1_4_m)+SUM(mhfm.pos_1_4_f) as pos_0_4,
                    SUM(mhfm.pos_5_9_m)+SUM(mhfm.pos_5_9_f)+SUM(mhfm.pos_10_14_m)+SUM(mhfm.pos_10_14_f) as pos_5_14,
                    SUM(mhfm.pos_15_64_m)+SUM(mhfm.pos_15_64_f) as pos_15_64,
                    SUM(mhfm.pos_diatas_64_m)+SUM(mhfm.pos_diatas_64_f) as pos_diatas_64,
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


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host="0.0.0.0", port=5000)
