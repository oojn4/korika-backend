from app import db

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
    pos_0_4 = db.Column(db.Integer)  # Age group 0-4 years
    pos_5_14 = db.Column(db.Integer)  # Age group 5-14 years
    pos_15_64 = db.Column(db.Integer)  # Age group 15-64 years
    pos_diatas_64 = db.Column(db.Integer)  # Age group above 64 years
    tot_pos = db.Column(db.Integer)  # Total positive cases
    kematian_malaria = db.Column(db.Integer)
    hamil_pos = db.Column(db.Integer)
    p_pf = db.Column(db.Integer)
    p_pv = db.Column(db.Integer)
    p_po = db.Column(db.Integer)
    p_pm = db.Column(db.Integer)
    p_pk = db.Column(db.Integer)
    p_mix = db.Column(db.Integer)
    p_suspek_pk = db.Column(db.Integer)
    kasus_pe = db.Column(db.Integer)
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
    
    # Weather data - only the specified fields
    hujan_hujan_mean = db.Column(db.Float)  # Mean rainfall
    hujan_hujan_max = db.Column(db.Float)  # Maximum rainfall
    hujan_hujan_min = db.Column(db.Float)  # Minimum rainfall
    tm_tm_mean = db.Column(db.Float)  # Mean temperature
    tm_tm_max = db.Column(db.Float)  # Maximum temperature
    tm_tm_min = db.Column(db.Float)  # Minimum temperature
    ss_monthly_mean = db.Column(db.Float)  # Mean sunshine duration
    ff_x_monthly_mean = db.Column(db.Float)  # Mean maximum wind speed
    ddd_x_monthly_mean = db.Column(db.Float)  # Mean wind direction
    ff_avg_monthly_mean = db.Column(db.Float)  # Mean average wind speed
    
    # Population data
    pop_penduduk_kab = db.Column(db.Integer)  # Population of district/kabupaten

    def to_dict(self):
        return {
            "id_mhfm": self.id_mhfm,
            "id_faskes": self.id_faskes,
            "bulan": self.bulan,
            "tahun": self.tahun,
            "konfirmasi_lab_mikroskop": self.konfirmasi_lab_mikroskop,
            "konfirmasi_lab_rdt": self.konfirmasi_lab_rdt,
            "konfirmasi_lab_pcr": self.konfirmasi_lab_pcr,
            "pos_0_4": self.pos_0_4,
            "pos_5_14": self.pos_5_14,
            "pos_15_64": self.pos_15_64,
            "pos_diatas_64": self.pos_diatas_64,
            "tot_pos": self.tot_pos,
            "kematian_malaria": self.kematian_malaria,
            "hamil_pos": self.hamil_pos,
            "p_pf": self.p_pf,
            "p_pv": self.p_pv,
            "p_po": self.p_po,
            "p_pm": self.p_pm,
            "p_pk": self.p_pk,
            "p_mix": self.p_mix,
            "p_suspek_pk": self.p_suspek_pk,
            "obat_standar": self.obat_standar,
            "obat_nonprogram": self.obat_nonprogram,
            "obat_primaquin": self.obat_primaquin,
            "kasus_pe": self.kasus_pe,
            "penularan_indigenus": self.penularan_indigenus,
            "penularan_impor": self.penularan_impor,
            "penularan_induced": self.penularan_induced,
            "relaps": self.relaps,
            "indikator_pengobatan_standar": self.indikator_pengobatan_standar,
            "indikator_primaquin": self.indikator_primaquin,
            "indikator_kasus_pe": self.indikator_kasus_pe,
            "status": self.status,
            
            # Weather data - only the specified fields
            "hujan_hujan_mean": self.hujan_hujan_mean,
            "hujan_hujan_max": self.hujan_hujan_max,
            "hujan_hujan_min": self.hujan_hujan_min,
            "tm_tm_mean": self.tm_tm_mean,
            "tm_tm_max": self.tm_tm_max,
            "tm_tm_min": self.tm_tm_min,
            "ss_monthly_mean": self.ss_monthly_mean,
            "ff_x_monthly_mean": self.ff_x_monthly_mean,
            "ddd_x_monthly_mean": self.ddd_x_monthly_mean,
            "ff_avg_monthly_mean": self.ff_avg_monthly_mean,
            
            # Population data
            "pop_penduduk_kab": self.pop_penduduk_kab
        }

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
    