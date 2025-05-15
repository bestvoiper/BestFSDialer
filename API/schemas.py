from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime

class CampanaBase(BaseModel):
    base: str
    id: Optional[int] = None
    cant: Optional[int] = None
    reintentos: Optional[int] = None
    horarios: Optional[str] = None
    plan: Optional[str] = 'default'
    loop: Optional[int] = None
    controlcorte: Optional[str] = None
    controlestado: Optional[str] = ''
    mail: Optional[str] = None
    blacklist: Optional[str] = 'S'
    activo: Optional[str] = 'N'
    camposaux: Optional[int] = 0
    rel_ivr: Optional[str] = None
    detectar_amd: Optional[str] = 'S'
    cantllamseg: Optional[int] = 1
    camposauxc: Optional[str] = None
    rel_cola: Optional[str] = None
    sobredisc: Optional[str] = '0|1|0|0'
    filtros: Optional[str] = None
    archsubido: Optional[str] = None
    fechacarga: Optional[datetime] = None
    outcid: Optional[str] = None
    ti: Optional[str] = 's'
    pc: Optional[str] = 'n|n'
    archivado: Optional[str] = 'n'
    inscrm: Optional[str] = 'N###1###...'
    tpfs: Optional[str] = '...###...'
    llamarigual: Optional[str] = 'N|N'

    @validator('fechacarga', pre=True)
    def parse_fechacarga(cls, v):
        if v == '0000-00-00 00:00:00' or not v:
            return None
        return v

    class Config:
        from_attributes = True

class CampanaCreate(CampanaBase):
    pass

class Campana(CampanaBase):
    pass

class DiscadorRequest(BaseModel):
    nombre_campana: str
    cps: Optional[int] = 50 