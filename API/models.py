from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.dialects.mysql import INTEGER
from database import Base

class Campana(Base):
    __tablename__ = 'bases'
    
    base = Column(String(80), primary_key=True)
    id = Column(INTEGER(unsigned=True))
    cant = Column(INTEGER(unsigned=True))
    reintentos = Column(INTEGER(unsigned=True))
    horarios = Column(Text)
    plan = Column(String(80), default='default')
    loop = Column(INTEGER(unsigned=True))
    controlcorte = Column(Text)
    controlestado = Column(Text, default='')
    mail = Column(String(255))
    blacklist = Column(String(1), default='S')
    activo = Column(String(1), default='N')
    camposaux = Column(INTEGER(unsigned=True), default=0)
    rel_ivr = Column(String(128))
    detectar_amd = Column(String(1), default='S')
    cantllamseg = Column(INTEGER(unsigned=True), default=1)
    camposauxc = Column(String(255))
    rel_cola = Column(String(128))
    sobredisc = Column(String(32), default='0|1|0|0')
    filtros = Column(Text)
    archsubido = Column(String(255))
    fechacarga = Column(DateTime, default='0000-00-00 00:00:00')
    outcid = Column(String(64))
    ti = Column(String(1), default='s')
    pc = Column(String(3), default='n|n')
    archivado = Column(String(1), default='n')
    inscrm = Column(String(64), default='N###1###...')
    tpfs = Column(String(255), default='...###...')
    llamarigual = Column(String(3), default='N|N')

class NumerosCampana:
    """Clase dinámica para manejar las tablas de números"""
    def __init__(self, table_name):
        self.__tablename__ = table_name
        self.id = Column(Integer, primary_key=True, index=True)
        self.telefono = Column(String(20))
        self.estado = Column(String(50))
        self.intentos = Column(Integer, default=0)
        self.ultima_llamada = Column(DateTime)
        self.resultado = Column(String(255)) 