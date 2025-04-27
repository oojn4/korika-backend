from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy import event
from app import db

# Base model class with timestamps and user tracking
class BaseModel(db.Model):
    __abstract__ = True
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    updated_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    
    def to_dict(self):
        """Base method to convert model to dictionary"""
        # This will be overridden by each child class
        result = {}
        for column in self.__table__.columns:
            result[column.name] = getattr(self, column.name)
        return result

# Master Province Table
class MasterProv(BaseModel):
    __tablename__ = 'masterprov'
    
    kd_prov = db.Column(db.String(10), primary_key=True)
    provinsi = db.Column(db.String(100), nullable=False)
    kd_bmkg = db.Column(db.String(10), nullable=False)
    
    # Relationships
    kabupaten = db.relationship('MasterKab', backref='province', lazy=True)
    kecamatan = db.relationship('MasterKec', backref='province', lazy=True)
    health_facilities = db.relationship('HealthFacility', backref='province_rel', lazy=True)
    population_data = db.relationship('Population', backref='province_pop', lazy=True)
    dbd_data = db.relationship('DBD', backref='province_dbd', lazy=True)
    lepto_data = db.relationship('Lepto', backref='province_lepto', lazy=True)
    
    def to_dict(self):
        return {
            'kd_prov': self.kd_prov,
            'kd_bmkg': self.kd_bmkg,
            'provinsi': self.provinsi,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<Province {self.provinsi}>'

# Master Kabupaten/District Table
class MasterKab(BaseModel):
    __tablename__ = 'masterkab'
    
    kd_kab = db.Column(db.String(10), primary_key=True)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'), nullable=False)
    kabkot = db.Column(db.String(100), nullable=False)
    kd_bmkg = db.Column(db.String(10), nullable=False)
    status_endemis = db.Column(db.String(100), nullable=False)
    
    # Relationships
    kecamatan = db.relationship('MasterKec', backref='district', lazy=True)
    health_facilities = db.relationship('HealthFacility', backref='district_rel', lazy=True)
    population_data = db.relationship('Population', backref='district_pop', lazy=True)
    dbd_data = db.relationship('DBD', backref='district_dbd', lazy=True)
    lepto_data = db.relationship('Lepto', backref='district_lepto', lazy=True)
    
    def to_dict(self):
        return {
            'kd_kab': self.kd_kab,
            'kd_prov': self.kd_prov,
            'kd_bmkg': self.kd_bmkg,
            'kabkot': self.kabkot,
            'status_endemis': self.status_endemis,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<District {self.kabkot}>'

# Master Kecamatan/Sub-district Table
class MasterKec(BaseModel):
    __tablename__ = 'masterkec'
    
    kd_kec = db.Column(db.String(10), primary_key=True)
    kd_kab = db.Column(db.String(10), db.ForeignKey('masterkab.kd_kab'), nullable=False)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'), nullable=False)
    kecamatan = db.Column(db.String(100), nullable=False)
    kd_bmkg = db.Column(db.String(10), nullable=False)
    
    
    # Relationships
    health_facilities = db.relationship('HealthFacility', backref='subdistrict_rel', lazy=True)
    population_data = db.relationship('Population', backref='subdistrict_pop', lazy=True)
    
    def to_dict(self):
        return {
            'kd_kec': self.kd_kec,
            'kd_kab': self.kd_kab,
            'kd_prov': self.kd_prov,
            'kd_bmkg': self.kd_bmkg,
            'kecamatan': self.kecamatan,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<Subdistrict {self.kecamatan}>'

# Health Facility Table
class HealthFacility(BaseModel):
    __tablename__ = 'healthfacility'
    
    id_faskes = db.Column(db.Integer, primary_key=True)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'), nullable=False)
    kd_kab = db.Column(db.String(10), db.ForeignKey('masterkab.kd_kab'), nullable=False)
    kd_kec = db.Column(db.String(10), db.ForeignKey('masterkec.kd_kec'), nullable=False)
    owner = db.Column(db.String(100))
    tipe_faskes = db.Column(db.String(50))
    nama_faskes = db.Column(db.String(200), nullable=False)
    address = db.Column(db.Text)
    url = db.Column(db.Text)
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    
    # Relationships
    malaria_reports = db.relationship('MalariaMonthly', backref='facility', lazy=True)
    env_factors = db.relationship('SocioEnvironmentalFactorsMonthly', backref='facility', lazy=True)
    
    def to_dict(self):
        return {
            'id_faskes': self.id_faskes,
            'kd_prov': self.kd_prov,
            'kd_kab': self.kd_kab,
            'kd_kec': self.kd_kec,
            'owner': self.owner,
            'tipe_faskes': self.tipe_faskes,
            'nama_faskes': self.nama_faskes,
            'address': self.address,
            'url': self.url,
            'lat': self.lat,
            'lon': self.lon,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<HealthFacility {self.nama_faskes}>'

# Malaria Monthly Report Table
class MalariaMonthly(BaseModel):
    __tablename__ = 'malariamonthly'
    
    id_mhfm = db.Column(db.Integer, primary_key=True)
    id_faskes = db.Column(db.Integer, db.ForeignKey('healthfacility.id_faskes'), nullable=False)
    bulan = db.Column(db.Integer, nullable=False)
    tahun = db.Column(db.Integer, nullable=False)
    konfirmasi_lab_mikroskop = db.Column(db.Integer)
    konfirmasi_lab_rdt = db.Column(db.Integer)
    konfirmasi_lab_pcr = db.Column(db.Integer)
    total_konfirmasi_lab = db.Column(db.Integer)
    pos_0_4 = db.Column(db.Integer)
    pos_5_14 = db.Column(db.Integer)
    pos_15_64 = db.Column(db.Integer)
    pos_diatas_64 = db.Column(db.Integer)
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
    p_others = db.Column(db.Integer)
    kasus_pe = db.Column(db.Integer)
    obat_standar = db.Column(db.Integer)
    obat_nonprogram = db.Column(db.Integer)
    obat_primaquin = db.Column(db.Integer)
    penularan_indigenus = db.Column(db.Integer)
    penularan_impor = db.Column(db.Integer)
    penularan_induced = db.Column(db.Integer)
    relaps = db.Column(db.Integer)
    indikator_pengobatan_standar = db.Column(db.Integer)
    indikator_primaquin = db.Column(db.Integer)
    indikator_kasus_pe = db.Column(db.Integer)
    status = db.Column(db.String(50))
    
    __table_args__ = (
        db.UniqueConstraint('id_mhfm', 'id_faskes', 'bulan', 'tahun', 'status', name='uk_malaria_monthly_new'),
    )
    
    def to_dict(self):
        return {
            'id_mhfm': self.id_mhfm,
            'id_faskes': self.id_faskes,
            'bulan': self.bulan,
            'tahun': self.tahun,
            'konfirmasi_lab_mikroskop': self.konfirmasi_lab_mikroskop,
            'konfirmasi_lab_rdt': self.konfirmasi_lab_rdt,
            'konfirmasi_lab_pcr': self.konfirmasi_lab_pcr,
            'total_konfirmasi_lab': self.total_konfirmasi_lab,
            'pos_0_4': self.pos_0_4,
            'pos_5_14': self.pos_5_14,
            'pos_15_64': self.pos_15_64,
            'pos_diatas_64': self.pos_diatas_64,
            'tot_pos': self.tot_pos,
            'kematian_malaria': self.kematian_malaria,
            'hamil_pos': self.hamil_pos,
            'p_pf': self.p_pf,
            'p_pv': self.p_pv,
            'p_po': self.p_po,
            'p_pm': self.p_pm,
            'p_pk': self.p_pk,
            'p_mix': self.p_mix,
            'p_suspek_pk': self.p_suspek_pk,
            'p_others': self.p_others,
            'kasus_pe': self.kasus_pe,
            'obat_standar': self.obat_standar,
            'obat_nonprogram': self.obat_nonprogram,
            'obat_primaquin': self.obat_primaquin,
            'penularan_indigenus': self.penularan_indigenus,
            'penularan_impor': self.penularan_impor,
            'penularan_induced': self.penularan_induced,
            'relaps': self.relaps,
            'indikator_pengobatan_standar': self.indikator_pengobatan_standar,
            'indikator_primaquin': self.indikator_primaquin,
            'indikator_kasus_pe': self.indikator_kasus_pe,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<MalariaMonthly {self.id_faskes} {self.bulan}/{self.tahun}>'

# Socio-Environmental Factors Monthly Table
class SocioEnvironmentalFactorsMonthly(BaseModel):
    __tablename__ = 'socioenvironmentalfactorsmonthly'
    
    id_env_factor = db.Column(db.Integer, primary_key=True)
    id_faskes = db.Column(db.Integer, db.ForeignKey('healthfacility.id_faskes'), nullable=False)
    bulan = db.Column(db.Integer, nullable=False)
    tahun = db.Column(db.Integer, nullable=False)
    hujan_hujan_mean = db.Column(db.Float)
    hujan_hujan_max = db.Column(db.Float)
    hujan_hujan_min = db.Column(db.Float)
    tm_tm_mean = db.Column(db.Float)
    tm_tm_max = db.Column(db.Float)
    tm_tm_min = db.Column(db.Float)
    rh_mean = db.Column(db.Float)
    rh_max = db.Column(db.Float)
    rh_min = db.Column(db.Float)
    ss_monthly_mean = db.Column(db.Float)
    ff_x_monthly_mean = db.Column(db.Float)
    ddd_x_monthly_mean = db.Column(db.Float)
    ff_avg_monthly_mean = db.Column(db.Float)
    
    __table_args__ = (
        db.UniqueConstraint('id_env_factor', 'id_faskes', 'bulan', 'tahun', name='uk_env_factors_monthly_new'),
    )
    
    def to_dict(self):
        return {
            'id_env_factor': self.id_env_factor,
            'id_faskes': self.id_faskes,
            'bulan': self.bulan,
            'tahun': self.tahun,
            'hujan_hujan_mean': self.hujan_hujan_mean,
            'hujan_hujan_max': self.hujan_hujan_max,
            'hujan_hujan_min': self.hujan_hujan_min,
            'tm_tm_mean': self.tm_tm_mean,
            'tm_tm_max': self.tm_tm_max,
            'tm_tm_min': self.tm_tm_min,
            'rh_mean': self.rh_mean,
            'rh_max': self.rh_max,
            'rh_min': self.rh_min,
            'ss_monthly_mean': self.ss_monthly_mean,
            'ff_x_monthly_mean': self.ff_x_monthly_mean,
            'ddd_x_monthly_mean': self.ddd_x_monthly_mean,
            'ff_avg_monthly_mean': self.ff_avg_monthly_mean,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<EnvFactors {self.id_faskes} {self.bulan}/{self.tahun}>'

# User Table
class User(db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    full_name = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    phone_number = db.Column(db.String(20))
    address_1 = db.Column(db.String(255))
    address_2 = db.Column(db.String(255))
    access_level = db.Column(db.String(20), nullable=False)
    
    def to_dict(self):
        return {
            'id': self.id,
            'email': self.email,
            'full_name': self.full_name,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'phone_number': self.phone_number,
            'address_1': self.address_1,
            'address_2': self.address_2,
            'access_level': self.access_level
        }
    
    def __repr__(self):
        return f'<User {self.email}>'

# Population Table
class Population(BaseModel):
    __tablename__ = 'population'
    
    id_population = db.Column(db.Integer, primary_key=True)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'))
    kd_kab = db.Column(db.String(10), db.ForeignKey('masterkab.kd_kab'))
    kd_kec = db.Column(db.String(10), db.ForeignKey('masterkec.kd_kec'))
    tahun = db.Column(db.Integer, nullable=False)
    jumlah_populasi = db.Column(db.Integer, nullable=False)
    
    def to_dict(self):
        return {
            'id_population': self.id_population,
            'kd_prov': self.kd_prov,
            'kd_kab': self.kd_kab,
            'kd_kec': self.kd_kec,
            'tahun': self.tahun,
            'jumlah_populasi': self.jumlah_populasi,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        level = "Province"
        code = self.kd_prov
        if self.kd_kec:
            level = "Subdistrict"
            code = self.kd_kec
        elif self.kd_kab:
            level = "District"
            code = self.kd_kab
        return f'<Population {level} {code} {self.tahun}: {self.jumlah_populasi}>'

# DBD (Dengue Hemorrhagic Fever) Table
class DBD(BaseModel):
    __tablename__ = 'dbd'
    
    id_dbd = db.Column(db.Integer, primary_key=True)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'), nullable=False)
    kd_kab = db.Column(db.String(10), db.ForeignKey('masterkab.kd_kab'), nullable=False)
    tahun = db.Column(db.Integer, nullable=False)
    bulan = db.Column(db.Integer, nullable=False)
    DBD_P = db.Column(db.Integer)  # Positive cases
    DBD_M = db.Column(db.Integer)  # Mortality cases
    status = db.Column(db.String(50))
    
    __table_args__ = (
        db.UniqueConstraint('id_dbd', 'bulan', 'tahun', 'status', name='uk_dbd'),
    )
    
    def to_dict(self):
        return {
            'id_dbd': self.id_dbd,
            'kd_prov': self.kd_prov,
            'kd_kab': self.kd_kab,
            'tahun': self.tahun,
            'bulan': self.bulan,
            'DBD_P': self.DBD_P,
            'DBD_M': self.DBD_M,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<DBD {self.kd_kab} {self.bulan}/{self.tahun}>'

# Lepto (Leptospirosis) Table
class Lepto(BaseModel):
    __tablename__ = 'lepto'
    
    id_lepto = db.Column(db.Integer, primary_key=True)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'), nullable=False)
    kd_kab = db.Column(db.String(10), db.ForeignKey('masterkab.kd_kab'), nullable=False)
    tahun = db.Column(db.Integer, nullable=False)
    bulan = db.Column(db.Integer, nullable=False)
    LEP_K = db.Column(db.Integer)  # Positive cases
    LEP_M = db.Column(db.Integer)  # Mortality cases
    status = db.Column(db.String(50))
    
    __table_args__ = (
        db.UniqueConstraint('id_lepto', 'bulan', 'tahun', 'status', name='uk_lepto'),
    )
    
    def to_dict(self):
        return {
            'id_lepto': self.id_lepto,
            'kd_prov': self.kd_prov,
            'kd_kab': self.kd_kab,
            'tahun': self.tahun,
            'bulan': self.bulan,
            'LEP_K': self.LEP_K,
            'LEP_M': self.LEP_M,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<Lepto {self.kd_kab} {self.bulan}/{self.tahun}>'
class ClimateMonthly(BaseModel):
    __tablename__ = 'climatemonthly'
    
    id_climate = db.Column(db.Integer, primary_key=True)
    kd_prov = db.Column(db.String(10), db.ForeignKey('masterprov.kd_prov'), nullable=False)
    kd_kab = db.Column(db.String(10), db.ForeignKey('masterkab.kd_kab'), nullable=False)
    kd_kec = db.Column(db.String(10), db.ForeignKey('masterkec.kd_kec'), nullable=False)
    tahun = db.Column(db.Integer, nullable=False)
    bulan = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(50))  # Added this based on the unique constraint
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'))
    updated_by = db.Column(db.Integer, db.ForeignKey('users.id'))
    
    # Hujan and temperature columns
    hujan_hujan_mean = db.Column(db.Float)
    hujan_hujan_max = db.Column(db.Float)
    hujan_hujan_min = db.Column(db.Float)
    rh_rh_mean = db.Column(db.Float)
    rh_rh_max = db.Column(db.Float)
    rh_rh_min = db.Column(db.Float)
    tm_tm_mean = db.Column(db.Float)
    tm_tm_max = db.Column(db.Float)
    tm_tm_min = db.Column(db.Float)
    
    # Max value columns
    max_value_10u = db.Column(db.Float)
    max_value_10v = db.Column(db.Float)
    max_value_2d = db.Column(db.Float)
    max_value_2t = db.Column(db.Float)
    max_value_cp = db.Column(db.Float)
    max_value_crr = db.Column(db.Float)
    max_value_cvh = db.Column(db.Float)
    max_value_cvl = db.Column(db.Float)
    max_value_e = db.Column(db.Float)
    max_value_lmlt = db.Column(db.Float)
    max_value_msl = db.Column(db.Float)
    max_value_ro = db.Column(db.Float)
    max_value_skt = db.Column(db.Float)
    max_value_sp = db.Column(db.Float)
    max_value_sro = db.Column(db.Float)
    max_value_swvl1 = db.Column(db.Float)
    max_value_tcc = db.Column(db.Float)
    max_value_tcrw = db.Column(db.Float)
    max_value_tcwv = db.Column(db.Float)
    max_value_tp = db.Column(db.Float)
    
    # Mean value columns
    mean_value_10u = db.Column(db.Float)
    mean_value_10v = db.Column(db.Float)
    mean_value_2d = db.Column(db.Float)
    mean_value_2t = db.Column(db.Float)
    mean_value_cp = db.Column(db.Float)
    mean_value_crr = db.Column(db.Float)
    mean_value_cvh = db.Column(db.Float)
    mean_value_cvl = db.Column(db.Float)
    mean_value_e = db.Column(db.Float)
    mean_value_lmlt = db.Column(db.Float)
    mean_value_msl = db.Column(db.Float)
    mean_value_ro = db.Column(db.Float)
    mean_value_skt = db.Column(db.Float)
    mean_value_sp = db.Column(db.Float)
    mean_value_sro = db.Column(db.Float)
    mean_value_swvl1 = db.Column(db.Float)
    mean_value_tcc = db.Column(db.Float)
    mean_value_tcrw = db.Column(db.Float)
    mean_value_tcwv = db.Column(db.Float)
    mean_value_tp = db.Column(db.Float)
    
    # Min value columns
    min_value_10u = db.Column(db.Float)
    min_value_10v = db.Column(db.Float)
    min_value_2d = db.Column(db.Float)
    min_value_2t = db.Column(db.Float)
    min_value_cp = db.Column(db.Float)
    min_value_crr = db.Column(db.Float)
    min_value_cvh = db.Column(db.Float)
    min_value_cvl = db.Column(db.Float)
    min_value_e = db.Column(db.Float)
    min_value_lmlt = db.Column(db.Float)
    min_value_msl = db.Column(db.Float)
    min_value_ro = db.Column(db.Float)
    min_value_skt = db.Column(db.Float)
    min_value_sp = db.Column(db.Float)
    min_value_sro = db.Column(db.Float)
    min_value_swvl1 = db.Column(db.Float)
    min_value_tcc = db.Column(db.Float)
    min_value_tcrw = db.Column(db.Float)
    min_value_tcwv = db.Column(db.Float)
    min_value_tp = db.Column(db.Float)
    
    # Additional columns
    buffer_used = db.Column(db.Integer)
    source_year = db.Column(db.Integer)
    
    # Relationships
    masterprov = db.relationship('MasterProv', foreign_keys=[kd_prov])
    masterkab = db.relationship('MasterKab', foreign_keys=[kd_kab])
    creator = db.relationship('User', foreign_keys=[created_by])
    updater = db.relationship('User', foreign_keys=[updated_by])
    
    # Unique constraint - updated to match the SQL provided
    __table_args__ = (
        db.UniqueConstraint('id_climate', 'bulan', 'tahun', 'status', name='uk_climatemonthly'),
    )
    
    def to_dict(self):
        return {
            'id_climate': self.id_climate,
            'kd_prov': self.kd_prov,
            'kd_kab': self.kd_kab,
            'kd_kec': self.kd_kec,
            'tahun': self.tahun,
            'bulan': self.bulan,
            'status': self.status,
            'hujan_hujan_mean': self.hujan_hujan_mean,
            'hujan_hujan_max': self.hujan_hujan_max,
            'hujan_hujan_min': self.hujan_hujan_min,
            'rh_rh_mean': self.rh_rh_mean,
            'rh_rh_max': self.rh_rh_max,
            'rh_rh_min': self.rh_rh_min,
            'tm_tm_mean': self.tm_tm_mean,
            'tm_tm_max': self.tm_tm_max,
            'tm_tm_min': self.tm_tm_min,
            'max_value_10u': self.max_value_10u,
            'max_value_10v': self.max_value_10v,
            'max_value_2d': self.max_value_2d,
            'max_value_2t': self.max_value_2t,
            'max_value_cp': self.max_value_cp,
            'max_value_crr': self.max_value_crr,
            'max_value_cvh': self.max_value_cvh,
            'max_value_cvl': self.max_value_cvl,
            'max_value_e': self.max_value_e,
            'max_value_lmlt': self.max_value_lmlt,
            'max_value_msl': self.max_value_msl,
            'max_value_ro': self.max_value_ro,
            'max_value_skt': self.max_value_skt,
            'max_value_sp': self.max_value_sp,
            'max_value_sro': self.max_value_sro,
            'max_value_swvl1': self.max_value_swvl1,
            'max_value_tcc': self.max_value_tcc,
            'max_value_tcrw': self.max_value_tcrw,
            'max_value_tcwv': self.max_value_tcwv,
            'max_value_tp': self.max_value_tp,
            'mean_value_10u': self.mean_value_10u,
            'mean_value_10v': self.mean_value_10v,
            'mean_value_2d': self.mean_value_2d,
            'mean_value_2t': self.mean_value_2t,
            'mean_value_cp': self.mean_value_cp,
            'mean_value_crr': self.mean_value_crr,
            'mean_value_cvh': self.mean_value_cvh,
            'mean_value_cvl': self.mean_value_cvl,
            'mean_value_e': self.mean_value_e,
            'mean_value_lmlt': self.mean_value_lmlt,
            'mean_value_msl': self.mean_value_msl,
            'mean_value_ro': self.mean_value_ro,
            'mean_value_skt': self.mean_value_skt,
            'mean_value_sp': self.mean_value_sp,
            'mean_value_sro': self.mean_value_sro,
            'mean_value_swvl1': self.mean_value_swvl1,
            'mean_value_tcc': self.mean_value_tcc,
            'mean_value_tcrw': self.mean_value_tcrw,
            'mean_value_tcwv': self.mean_value_tcwv,
            'mean_value_tp': self.mean_value_tp,
            'min_value_10u': self.min_value_10u,
            'min_value_10v': self.min_value_10v,
            'min_value_2d': self.min_value_2d,
            'min_value_2t': self.min_value_2t,
            'min_value_cp': self.min_value_cp,
            'min_value_crr': self.min_value_crr,
            'min_value_cvh': self.min_value_cvh,
            'min_value_cvl': self.min_value_cvl,
            'min_value_e': self.min_value_e,
            'min_value_lmlt': self.min_value_lmlt,
            'min_value_msl': self.min_value_msl,
            'min_value_ro': self.min_value_ro,
            'min_value_skt': self.min_value_skt,
            'min_value_sp': self.min_value_sp,
            'min_value_sro': self.min_value_sro,
            'min_value_swvl1': self.min_value_swvl1,
            'min_value_tcc': self.min_value_tcc,
            'min_value_tcrw': self.min_value_tcrw,
            'min_value_tcwv': self.min_value_tcwv,
            'min_value_tp': self.min_value_tp,
            'buffer_used': self.buffer_used,
            'source_year': self.source_year,
            'created_by': self.created_by,
            'updated_by': self.updated_by
        }
    
    def __repr__(self):
        return f'<ClimateMonthly {self.kd_prov}/{self.kd_kab}/{self.kd_kec} {self.bulan}/{self.tahun}>'