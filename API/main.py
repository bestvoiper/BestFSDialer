from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData, Table, select, text
from datetime import datetime
import asyncio
from typing import List

from database import get_db, engine
from schemas import DiscadorRequest, Campana as CampanaSchema
from models import Campana
from dialer import send_all_calls_persistent

app = FastAPI(title="API Discador")

@app.get("/campanas/", response_model=List[CampanaSchema])
async def listar_campanas(db: Session = Depends(get_db)):
    """Lista todas las campañas disponibles"""
    return db.query(Campana).all()

@app.get("/campana/{nombre_campana}", response_model=CampanaSchema)
async def obtener_campana(nombre_campana: str, db: Session = Depends(get_db)):
    """Obtiene los detalles de una campaña específica"""
    campana = db.query(Campana).filter(Campana.base == nombre_campana).first()
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")
    return campana

@app.post("/discador/iniciar/")
async def iniciar_discador(request: DiscadorRequest, db: Session = Depends(get_db)):
    """Inicia el discador para una campaña específica"""
    # Obtener la campaña
    campana = db.query(Campana).filter(Campana.base == request.nombre_campana).first()
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")

    # Crear tabla dinámica para los números
    metadata = MetaData()
    numeros_table = Table(
        campana.base,
        metadata,
        autoload_with=engine
    )

    # Obtener números pendientes
    query = select(numeros_table.c.telefono).where(
        (numeros_table.c.estado.is_(None) | (numeros_table.c.estado == 'n')) &
        (numeros_table.c.intentos < campana.reintentos)
    )
    
    result = db.execute(query)
    numeros = [row[0] for row in result]

    if not numeros:
        raise HTTPException(status_code=400, detail="No hay números pendientes para marcar")

    # Configurar destino según detectar_amd
    destino = "9110" if not campana.detectar_amd else "humano"

    try:
        # Actualizar estado de la campaña
        campana.controlestado = "EN_PROCESO"
        campana.fechacarga = datetime.now()
        db.commit()

        # Iniciar el discador de manera asíncrona
        asyncio.create_task(
            send_all_calls_persistent(
                numbers=numeros,
                cps=request.cps,
                destino=destino,
                campaign_name=request.nombre_campana
            )
        )

        return {"message": f"Discador iniciado para la campaña {request.nombre_campana}",
                "numeros_a_marcar": len(numeros)}

    except Exception as e:
        campana.controlestado = "ERROR"
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/discador/estado/{nombre_campana}")
async def estado_discador(nombre_campana: str, db: Session = Depends(get_db)):
    """Obtiene el estado actual del discador para una campaña"""
    campana = db.query(Campana).filter(Campana.base == nombre_campana).first()
    if not campana:
        raise HTTPException(status_code=404, detail="Campaña no encontrada")

    # Obtener estadísticas de la campaña
    metadata = MetaData()
    numeros_table = Table(campana.base, metadata, autoload_with=engine)
    
    stats_query = text(f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN estado = 'COMPLETADO' THEN 1 ELSE 0 END) as completados,
            SUM(CASE WHEN estado = 'PENDIENTE' THEN 1 ELSE 0 END) as pendientes,
            SUM(CASE WHEN estado = 'ERROR' THEN 1 ELSE 0 END) as errores
        FROM {campana.base}
    """)
    
    result = db.execute(stats_query).first()

    return {
        "nombre_campana": nombre_campana,
        "estado": campana.controlestado if campana.controlestado else "",
        "ultima_ejecucion": campana.fechacarga,
        "estadisticas": {
            "total": result[0] if result[0] else 0,
            "completados": result[1] if result[1] else 0,
            "pendientes": result[2] if result[2] else 0,
            "errores": result[3] if result[3] else 0
        }
    } 