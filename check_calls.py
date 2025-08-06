import asyncio
import time
import json
from datetime import datetime, time as dt_time
import re
from sqlalchemy import create_engine, text
import logging
import sys

def is_now_in_campaign_schedule(horario_str):
    """Verifica si el horario actual está dentro del rango permitido."""
    if not horario_str:
        return True  # Si no hay restricción, permitir

    try:
        # Separar días y horas
        dias, horas = horario_str.split('|')
        # Días: "1-5" (Lunes=1, Domingo=7)
        day_range = dias.split('-')
        if len(day_range) == 2:
            day_start, day_end = int(day_range[0]), int(day_range[1])
        else:
            day_start = day_end = int(day_range[0])
        
        # Horas: "0900-1800"
        hour_start = dt_time(int(horas[:2]), int(horas[2:4]))
        hour_end = dt_time(int(horas[5:7]), int(horas[7:9]))
        
        now = datetime.now()
        weekday = now.isoweekday()  # Lunes=1, Domingo=7
        now_time = now.time()
        
        # Verificar día y hora
        if not (day_start <= weekday <= day_end):
            return False
        if not (hour_start <= now_time <= hour_end):
            return False
        return True
    except Exception as e:
        logger.error(f"Error verificando horario '{horario_str}': {e}")
        return True  # Si hay error en el formato, permitir por defecto

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

FREESWITCH_HOST = "127.0.0.1"
FREESWITCH_PORT = 8021
FREESWITCH_PASSWORD = "1Pl}0F~~801l"
GATEWAY = "gw_pstn"
DB_URL = "mysql+pymysql://consultas:consultas@localhost/masivos"

try:
    import ESL
except ImportError:
    print("Debes instalar los bindings oficiales de FreeSWITCH ESL para Python (no el paquete ESL de PyPI).")
    exit(1)

try:
    from websocket_server import send_stats_to_websocket, send_event_to_websocket
    WEBSOCKET_AVAILABLE = True
    logger.info("✅ WebSocket disponible - estadísticas en vivo activadas")
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.info("⚠️ WebSocket no disponible - funcionando sin estadísticas en vivo")

async def safe_send_to_websocket(func, *args, **kwargs):
    if not WEBSOCKET_AVAILABLE:
        return
    try:
        await func(*args, **kwargs)
    except OSError as e:
        if hasattr(e, 'errno') and e.errno == 98:
            logger.warning("⚠️ El WebSocket ya está corriendo en el puerto 8765. Solo debe haber un proceso escuchando en ese puerto.")
        else:
            logger.warning(f"⚠️ Error enviando al WebSocket: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Error enviando al WebSocket: {e}")

class DialerStats:
    def __init__(self, campaign_name=None):
        self.campaign_name = campaign_name  # <-- Añade el nombre de campaña a cada objeto
        self.calls_sent = 0
        self.calls_answered = 0
        self.calls_failed = 0
        self.calls_busy = 0
        self.calls_no_answer = 0
        self.calls_ringing = 0
        self.ringing_numbers = set()
        self.active_numbers = set()
        # --- Llevar registro de números únicos por estado por campaña ---
        self.unique_no_answer = set()
        self.unique_failed = set()
        self.unique_busy = set()
        self.unique_answered = set()
        self.unique_sent = set()

    def print_live_stats(self, campaign_name=None):
        # Usa el nombre de campaña del objeto si no se pasa explícito
        campaign = campaign_name or self.campaign_name or ''
        total_processed = len(self.unique_answered) + len(self.unique_failed) + len(self.unique_busy) + len(self.unique_no_answer)
        sys.stdout.write(
            f"\r📊 [{campaign}] Enviadas: {len(self.unique_sent)} | Sonando: {len(self.ringing_numbers)} | Activas: {len(self.active_numbers)} | Contestadas: {len(self.unique_answered)} | Fallidas: {len(self.unique_failed)} | Ocupadas: {len(self.unique_busy)} | Sin respuesta: {len(self.unique_no_answer)} | Procesadas: {total_processed}"
        )
        sys.stdout.flush()

    def to_dict(self):
        return {
            "campaign_name": self.campaign_name or "",
            "calls_sent": len(self.unique_sent),
            "calls_answered": len(self.unique_answered),
            "calls_failed": len(self.unique_failed),
            "calls_busy": len(self.unique_busy),
            "calls_no_answer": len(self.unique_no_answer),
            "calls_ringing": len(self.ringing_numbers),
            "ringing_numbers": list(self.ringing_numbers),
            "active_calls": len(self.active_numbers),
            "active_numbers": list(self.active_numbers),
            "total_processed": len(self.unique_answered) + len(self.unique_failed) + len(self.unique_busy) + len(self.unique_no_answer)
        }

def extract_field(event_str, field):
    try:
        return event_str.split(f"{field}: ")[1].split("\n")[0].strip()
    except:
        return ""

def update_call_status(campaign_name, numero, estado, engine, duracion="0", incrementar_intento=None, max_intentos=None, uuid=None, hangup_reason=None):
    if not campaign_name:
        logger.error(f"Error actualizando estado: campaign_name vacío para número {numero}, estado {estado}")
        return
    try:
        with engine.begin() as conn:
            # --- Actualizar por UUID si está disponible ---
            where_clause = "uuid = :uuid" if uuid else "telefono = :numero"
            params = {"estado": estado, "duracion": duracion}
            if uuid:
                params["uuid"] = uuid
            else:
                params["numero"] = numero
            
            # Agregar hangup_reason si está disponible
            if hangup_reason:
                params["hangup_reason"] = hangup_reason

            if estado == "pendiente" or estado == "P":
                update_fields = "estado = :estado, duracion = :duracion, intentos = CASE WHEN intentos < 1 THEN 1 ELSE intentos END"
                if hangup_reason:
                    update_fields += ", hangup_reason = :hangup_reason"
                
                stmt = text(f"""
                    UPDATE {campaign_name}
                    SET {update_fields},
                        uuid = :uuid
                    WHERE telefono = :numero
                """) if not uuid else text(f"""
                    UPDATE {campaign_name}
                    SET {update_fields}
                    WHERE uuid = :uuid
                """)
                conn.execute(stmt, {**params, "numero": numero, "uuid": uuid})
            elif estado == "N" and max_intentos is not None:
                intentos_actual = conn.execute(
                    text(f"SELECT intentos FROM {campaign_name} WHERE {where_clause}"),
                    params
                ).scalar() or 0
                
                update_fields = "estado = :estado, duracion = :duracion"
                if hangup_reason:
                    update_fields += ", hangup_reason = :hangup_reason"
                
                if intentos_actual + 1 < max_intentos:
                    update_fields += ", intentos = intentos + 1"
                
                stmt = text(f"""
                    UPDATE {campaign_name}
                    SET {update_fields}
                    WHERE {where_clause}
                """)
                conn.execute(stmt, params)
            else:
                update_fields = "estado = :estado, duracion = :duracion"
                if hangup_reason:
                    update_fields += ", hangup_reason = :hangup_reason"
                
                stmt = text(f"""
                    UPDATE {campaign_name}
                    SET {update_fields}
                    WHERE {where_clause}
                """)
                conn.execute(stmt, params)
    except Exception as e:
        logger.error(f"Error actualizando estado en campaña '{campaign_name}': {e}")

# Guarda el tiempo de respuesta por llamada contestada
answered_times = {}

async def handle_events_inline(con, campaign_name, stats, engine, max_intentos):
    global answered_times
    destino_transfer = "9999"
    try:
        event = con.recvEventTimed(100)
        if not event:
            return
        event_str = event.serialize()
        
        numero = extract_field(event_str, "origination_caller_id_number")
        if not numero:
            numero = extract_field(event_str, "variable_callee_id_number")
        if not numero:
            numero = extract_field(event_str, "Caller-Destination-Number")
        if not numero:
            numero = extract_field(event_str, "Caller-Caller-ID-Number")
        uuid = extract_field(event_str, "Unique-ID")

        if numero == destino_transfer or not numero:
            logger.debug(f"Ignorando evento para número vacío o de transferencia: {numero}")
            return

        if "CHANNEL_PROGRESS_MEDIA" in event_str:
            logger.info(f"🔔 Llamada en progreso: {numero}")
            stats.ringing_numbers.add(numero)
            stats.active_numbers.add(numero)
            stats.print_live_stats(campaign_name)
            update_call_status(campaign_name, numero, "P", engine, uuid=uuid)
            
            await safe_send_to_websocket(send_event_to_websocket, "call_progress", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "status": "in_progress",
                "stats": stats.to_dict()
            })
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())

        elif "CHANNEL_ANSWER" in event_str:
            logger.info(f"📞 Llamada contestada: {numero}")
            stats.calls_answered += 1
            stats.ringing_numbers.discard(numero)
            stats.active_numbers.add(numero)
            update_call_status(campaign_name, numero, "S", engine, uuid=uuid)
            answered_times[(campaign_name, numero)] = time.time()
            stats.print_live_stats(campaign_name)
            
            await safe_send_to_websocket(send_event_to_websocket, "call_answered", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "stats": stats.to_dict()
            })
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())

            # Transferir después de contestar
            transfer_cmd = f"uuid_transfer {uuid} 9999 XML {campaign_name}"
            response = con.api(transfer_cmd)
            if "+OK" in response.getBody():
                logger.info(f"✅ Llamada transferida a 9999@{campaign_name} para {numero}")
                stats.calls_answered -= 1
                stats.active_numbers.discard(numero)
                await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())
            else:
                logger.warning(f"❌ Fallo al transferir llamada {numero}: {response.getBody()}")

        elif "CHANNEL_HANGUP_COMPLETE" in event_str:
            duracion = extract_field(event_str, "variable_duration")
            causa = extract_field(event_str, "Hangup-Cause")
            stats.ringing_numbers.discard(numero)
            stats.active_numbers.discard(numero)

            # 👉 Identificar quién colgó
            hangup_disposition = extract_field(event_str, "variable_sip_hangup_disposition")
            hangup_reason = ""
            if hangup_disposition == "recv_bye":
                logger.info(f"📴 El cliente colgó la llamada: {numero}")
                hangup_reason = "client_hangup"
            elif hangup_disposition == "send_bye":
                logger.info(f"📴 El servidor colgó la llamada: {numero}")
                hangup_reason = "server_hangup"
            elif hangup_disposition == "recv_cancel":
                logger.info(f"❌ El cliente canceló antes de contestar: {numero}")
                hangup_reason = "client_cancel"
            elif hangup_disposition == "send_cancel":
                logger.info(f"❌ El servidor canceló antes de contestar: {numero}")
                hangup_reason = "server_cancel"
            else:
                logger.info(f"🔍 Disposición de colgado desconocida o no SIP: {hangup_disposition}")
                hangup_reason = hangup_disposition or "unknown"

            try:
                with engine.connect() as conn_db:
                    query = text(f"SELECT estado, intentos FROM {campaign_name} WHERE uuid = :uuid")
                    result = conn_db.execute(query, {"uuid": uuid}).fetchone()
                    estado_actual = result[0] if result else None
                    intentos_actual = result[1] if result else 0
            except Exception as e:
                logger.error(f"Error consultando estado/intentos previos de {numero}: {e}")
                estado_actual = None
                intentos_actual = 0

            if causa == "NORMAL_CLEARING":
                logger.info(f"✅ Llamada completada: {numero}, Duración: {duracion}")
                real_duration = None
                if estado_actual == "S":
                    if (campaign_name, numero) in answered_times:
                        answered_at = answered_times.pop((campaign_name, numero))
                        real_duration = int(time.time() - answered_at)
                    update_call_status(
                        campaign_name, numero, "C", engine,
                        str(real_duration) if real_duration is not None else duracion,
                        incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason
                    )
                else:
                    update_call_status(campaign_name, numero, "C", engine, duracion, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_completed", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "duracion": str(real_duration) if real_duration is not None else duracion,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa in ["USER_BUSY", "CALL_REJECTED"]:
                logger.info(f"📵 Línea ocupada: {numero}")
                stats.calls_busy += 1
                stats.unique_busy.add(numero)
                update_call_status(campaign_name, numero, "O", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_busy", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa in ["NO_ANSWER", "ORIGINATOR_CANCEL"]:
                logger.info(f"📴 Sin respuesta: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(numero)
                update_call_status(
                    campaign_name, numero, "N", engine,
                    incrementar_intento=None, max_intentos=max_intentos, uuid=uuid, hangup_reason=hangup_reason
                )
                await safe_send_to_websocket(send_event_to_websocket, "call_no_answer", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa == "NO_USER_RESPONSE":
                logger.info(f"📴 Sin respuesta del usuario: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(numero)
                update_call_status(campaign_name, numero, "U", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_no_answer_user", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa == "NORMAL_TEMPORARY_FAILURE":
                logger.info(f"🚫 Sin canales disponibles: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(numero)
                update_call_status(campaign_name, numero, "E", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_no_channels", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa == "NO_ROUTE_DESTINATION":
                logger.info(f"🗺️ Sin ruta de destino: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(numero)
                update_call_status(campaign_name, numero, "R", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_no_route", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa == "UNALLOCATED_NUMBER":
                logger.info(f"❌ Número inexistente: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(numero)
                update_call_status(campaign_name, numero, "I", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_invalid_number", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa == "INCOMPATIBLE_DESTINATION":
                logger.info(f"🔧 Codecs incompatibles: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(numero)
                update_call_status(campaign_name, numero, "X", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_incompatible", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa == "RECOVERY_ON_TIMER_EXPIRE":
                logger.info(f"⏰ Timeout SIP: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(numero)
                update_call_status(campaign_name, numero, "T", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_timeout", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            else:
                logger.info(f"❌ Llamada fallida: {numero}, Causa: {causa}")
                stats.calls_failed += 1
                stats.unique_failed.add(numero)
                update_call_status(campaign_name, numero, "E", engine, incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                await safe_send_to_websocket(send_event_to_websocket, "call_failed", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "causa": causa,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })

            stats.print_live_stats(campaign_name)
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())

    except Exception as e:
        logger.error(f"Error en handle_events_inline: {e}")

async def send_all_calls_persistent(numbers, cps, destino, campaign_name, max_intentos, stats=None):
    if not 1 <= cps <= 100:
        raise ValueError(f"CPS fuera de rango permitido: {cps}")

    stats = stats or DialerStats(campaign_name=campaign_name)
    stats.campaign_name = campaign_name  # <-- Siempre asegura el nombre correcto
    uuid_map = {}
    delay = 1 / cps
    engine = create_engine(DB_URL)
    log = logger.info

    log(f"🚦 Enviando llamadas a {cps} CPS (~{delay:.3f}s entre originates)")
    valid_numbers = [n for n in numbers if n.strip().isdigit()]
    log(f"📦 {len(valid_numbers)} números válidos para campaña {campaign_name}")

    if WEBSOCKET_AVAILABLE:
        await safe_send_to_websocket(send_stats_to_websocket, {
            "campaign_name": campaign_name,
            "total_numbers": len(valid_numbers),
            "cps": cps,
            **stats.to_dict()
        })
        await asyncio.sleep(2)

    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
    if not con.connected():
        log("❌ No se pudo conectar a FreeSWITCH ESL.")
        return stats
    con.events("plain", "CHANNEL_ANSWER CHANNEL_HANGUP_COMPLETE CHANNEL_PROGRESS")
    log("✅ Conectado a FreeSWITCH ESL")

    start = time.time()
    total = len(valid_numbers)
    i = 0

    async def live_stats_updater():
        while True:
            await safe_send_to_websocket(send_stats_to_websocket, {
                "campaign_name": campaign_name,
                "total_numbers": len(valid_numbers),
                "cps": cps,
                **stats.to_dict()
            })
            if stats.active_numbers or stats.ringing_numbers or stats.calls_sent < len(valid_numbers):
                await asyncio.sleep(1)
            else:
                break

    stats_task = asyncio.create_task(live_stats_updater())

    # Procesa eventos en segundo plano mientras envía lotes
    async def event_processor():
        while stats.calls_sent < total or stats.active_numbers or stats.ringing_numbers:
            await handle_events_inline(con, campaign_name, stats, engine, max_intentos)
            await asyncio.sleep(0.01)
    event_task = asyncio.create_task(event_processor())

    while i < total:
        # 🕐 Validar horario antes de enviar cada lote
        try:
            with engine.connect() as conn_schedule:
                horario_result = conn_schedule.execute(text(f"SELECT horarios FROM campañas WHERE nombre = :nombre"), {"nombre": campaign_name}).fetchone()
                horario_actual = horario_result[0] if horario_result else None
                
                if horario_actual and not is_now_in_campaign_schedule(horario_actual):
                    log(f"⏸️ Campaña {campaign_name} fuera de horario ({horario_actual}) - pausando campaña")
                    
                    # Actualizar estado de la campaña a 'P' (pausada)
                    try:
                        with engine.begin() as conn_update:
                            conn_update.execute(text("UPDATE campañas SET activo = 'P' WHERE nombre = :nombre"), {"nombre": campaign_name})
                            log(f"✅ Campaña {campaign_name} marcada como pausada (P) en base de datos")
                    except Exception as e:
                        logger.error(f"Error actualizando estado de campaña {campaign_name} a pausada: {e}")
                    
                    await safe_send_to_websocket(send_event_to_websocket, "campaign_paused", {
                        "campaign": campaign_name,
                        "reason": "Fuera de horario",
                        "horario": horario_actual,
                        "status": "P",
                        "stats": stats.to_dict()
                    })
                    break  # Salir del bucle de envío de lotes
        except Exception as e:
            logger.error(f"Error verificando horario para {campaign_name}: {e}")
            # Continuar si hay error verificando horario
        
        batch = valid_numbers[i:i + cps]
        log(f"🚩 Enviando lote {i // cps + 1}: {len(batch)} llamadas")
        for numero in batch:
            uuid = f"{campaign_name}_{numero}_{int(time.time()*1000)}"
            uuid_map[numero] = uuid
            # Originate directo al contexto, sin &park()
            originate_str = (
                f"bgapi originate "
                f"{{origination_caller_id_name='Outbound',ignore_early_media=false,"
                f"origination_uuid={uuid},"
                f"campaign_name='{campaign_name}',"
                f"origination_caller_id_number='{numero}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} 2222 XML {campaign_name}"
            )
            con.api(originate_str)
            
            # Insertar el uuid en la base de datos al iniciar la llamada y actualizar fecha_envio
            try:
                with engine.begin() as conn:
                    stmt = text(f"UPDATE {campaign_name} SET uuid = :uuid, fecha_envio = :fecha_envio WHERE telefono = :numero")
                    conn.execute(stmt, {
                        "uuid": uuid,
                        "numero": numero,
                        "fecha_envio": datetime.now()
                    })
            except Exception as e:
                logger.error(f"Error insertando uuid/fecha_envio para {numero} en {campaign_name}: {e}")
            # Estado P: siempre poner intentos=1 si es la primera vez y guardar uuid
            update_call_status(campaign_name, numero, "Pendiente", engine, incrementar_intento=None, max_intentos=max_intentos, uuid=uuid)
            stats.calls_sent += 1
            stats.unique_sent.add(numero)
            await safe_send_to_websocket(send_event_to_websocket, "call_pending", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "status": "pending",
                "stats": stats.to_dict()
            })
            await safe_send_to_websocket(send_stats_to_websocket, {
                "campaign_name": campaign_name,
                "total_numbers": len(valid_numbers),
                "cps": cps,
                **stats.to_dict()
            })
            await asyncio.sleep(delay)
        i += cps

    # Espera a que terminen los eventos pendientes
    await event_task
    await stats_task

    max_wait = 120
    waited = 0
    while waited < max_wait:
        for _ in range(cps * 2):
            await handle_events_inline(con, campaign_name, stats, engine, max_intentos)
        await safe_send_to_websocket(send_stats_to_websocket, {
            "campaign_name": campaign_name,
            "total_numbers": len(valid_numbers),
            "cps": cps,
            **stats.to_dict()
        })
        # --- Verifica que todos los números tengan estado final actualizado ---
        with engine.connect() as conn_db:
            query = text(f"SELECT telefono, estado, uuid FROM {campaign_name} WHERE telefono IN :nums")
            result = conn_db.execute(query, {"nums": tuple(valid_numbers)})
            pendientes = 0
            for row in result:
                estado = row[1]
                uuid_val = row[2]
                # Si el estado es "S" (contestada), aún no está finalizada
                if estado == "S" or not uuid_val or estado not in ("C", "E", "O", "N", "U"):
                    pendientes += 1
        # 🔥 Actualiza stats.calls_answered con la BD para reflejar el valor real
        with engine.connect() as conn_db:
            query = text(f"SELECT COUNT(*) FROM {campaign_name} WHERE estado = 'S'")
            stats.calls_answered = conn_db.execute(query).scalar() or 0
        # Solo termina si todos los números tienen estado final y uuid asignado (no "S")
        if pendientes == 0 and not stats.active_numbers and not stats.ringing_numbers:
            break
        await asyncio.sleep(0.5)
        waited += 0.5

    await stats_task

    end = time.time()
    await safe_send_to_websocket(send_event_to_websocket, "campaign_finished", {
        "campaign_name": campaign_name,
        "total_sent": stats.calls_sent,
        "duration": end - start
    })
    log(f"\n✅ {stats.calls_sent} llamadas enviadas en {end - start:.2f}s")
    log(f"📈 Envío real: {stats.calls_sent / (end - start):.2f} CPS")
    log(f"📊 Estadísticas finales:")
    log(f"   📤 Enviadas: {stats.calls_sent}")
    log(f"   🔔 Sonando: {len(stats.ringing_numbers)}")
    log(f"   🟢 Activas: {len(stats.active_numbers)}")
    log(f"   📞 Contestadas: {stats.calls_answered}")
    log(f"   ❌ Fallidas: {stats.calls_failed}")
    log(f"   📵 Ocupadas: {stats.calls_busy}")
    log(f"   📴 Sin respuesta: {stats.calls_no_answer}")

    return stats

async def main():
    engine = create_engine(DB_URL)
    campaigns_data = {}
    
    # 🚀 Iniciar WebSocket server inmediatamente al arrancar el script
    if WEBSOCKET_AVAILABLE:
        try:
            from websocket_server import ws_server
            await ws_server.start_server()
            logger.info("🌐 WebSocket server iniciado y listo para recibir conexiones")
            
            # Enviar mensaje inicial de que el sistema está activo
            await safe_send_to_websocket(send_event_to_websocket, "system_started", {
                "message": "Sistema de monitoreo iniciado",
                "timestamp": datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"❌ Error iniciando WebSocket server: {e}")
    
    logger.info("🎯 Iniciando bucle principal de monitoreo de campañas...")

    while True:
        try:
            with engine.connect() as conn:
                # Solo seleccionar campañas tipo 'Audio', fecha_programada válida y horario actual permitido
                # INCLUIR campañas marcadas como 'F' (finalizadas) que hayan sido reactivadas a 'S'
                campaigns = conn.execute(
                    text("""
                        SELECT nombre, reintentos, horarios 
                        FROM campañas 
                        WHERE activo = 'S' 
                        AND tipo = 'Audio'
                        AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                    """)
                )
                
                # 🔄 Verificar campañas pausadas (P) para reactivarlas si entran en horario
                paused_campaigns = conn.execute(
                    text("""
                        SELECT nombre, reintentos, horarios 
                        FROM campañas 
                        WHERE activo = 'P' 
                        AND tipo = 'Audio'
                        AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                    """)
                )
                
                # Procesar campañas pausadas
                for row in paused_campaigns:
                    nombre, reintentos, horario = row[0], row[1], row[2] if len(row) > 2 else None
                    if is_now_in_campaign_schedule(horario):
                        logger.info(f"🔄 Campaña {nombre} pausada entrando en horario ({horario}) - reactivando")
                        try:
                            with engine.begin() as conn_reactivate:
                                conn_reactivate.execute(text("UPDATE campañas SET activo = 'S' WHERE nombre = :nombre"), {"nombre": nombre})
                                logger.info(f"✅ Campaña {nombre} reactivada automáticamente (P → S)")
                                
                                if WEBSOCKET_AVAILABLE:
                                    await safe_send_to_websocket(send_event_to_websocket, "campaign_reactivated", {
                                        "campaign_name": nombre,
                                        "reason": "Entrada en horario",
                                        "horario": horario,
                                        "previous_status": "P",
                                        "new_status": "S",
                                        "timestamp": datetime.now().isoformat()
                                    })
                        except Exception as e:
                            logger.error(f"Error reactivando campaña {nombre}: {e}")
                    else:
                        logger.debug(f"⏸️ Campaña pausada {nombre} aún fuera de horario ({horario})")
                
                # Filtrar campañas por horario permitido
                active_campaigns = []
                for row in campaigns:
                    nombre, reintentos, horario = row[0], row[1], row[2] if len(row) > 2 else None
                    if is_now_in_campaign_schedule(horario):
                        active_campaigns.append((nombre, reintentos))
                    else:
                        logger.info(f"⏸️ Campaña {nombre} fuera de horario ({horario}), no se marcará en este ciclo.")

            # Limpiar campañas inactivas y finalizar campañas
            inactive = set(campaigns_data.keys()) - set(c for c, _ in active_campaigns)
            for c in inactive:
                if WEBSOCKET_AVAILABLE:
                    await safe_send_to_websocket(send_event_to_websocket, "campaign_finished", {
                        "campaign_name": c,
                        "message": "Campaña finalizada"
                    })
                del campaigns_data[c]

            # 📡 Enviar heartbeat cada ciclo para mantener conexiones activas
            if WEBSOCKET_AVAILABLE:
                await safe_send_to_websocket(send_event_to_websocket, "system_heartbeat", {
                    "active_campaigns": len(active_campaigns),
                    "timestamp": datetime.now().isoformat(),
                    "status": "running"
                })

            num_active = len(active_campaigns)
            
            if num_active == 0:
                logger.info("⏸️ No hay campañas activas. WebSocket permanece activo...")
                # Enviar stats vacías para mantener la interfaz actualizada
                if WEBSOCKET_AVAILABLE:
                    await safe_send_to_websocket(send_stats_to_websocket, {
                        "type": "multi_campaign_stats",
                        "data": [],
                        "message": "No hay campañas activas"
                    })
                await asyncio.sleep(10)
                continue

            cps_global = 50
            cps_per_campaign = max(1, cps_global // num_active) if num_active > 0 else 1

            dialer_tasks = []
            for campaign_name, max_intentos in active_campaigns:
                if campaign_name not in campaigns_data:
                    campaigns_data[campaign_name] = {
                        "stats": None,
                        "ringing_numbers": [],
                        "active_numbers": [],
                        "answered_numbers": [],
                        "failed_numbers": [],
                        "busy_numbers": [],
                        "no_answer_numbers": [],
                        "stats_obj": DialerStats(campaign_name=campaign_name),
                    }

                stats = campaigns_data[campaign_name]["stats_obj"]
                stats.campaign_name = campaign_name

                # Obtener números pendientes de la base
                with engine.connect() as conn:
                    # --- NUEVO: Si la campaña tiene horario, solo marcar si está en horario ---
                    horario = None
                    try:
                        res = conn.execute(text(f"SELECT horarios FROM campañas WHERE nombre = :nombre"), {"nombre": campaign_name}).fetchone()
                        if res:
                            horario = res[0]
                    except Exception:
                        pass
                    if horario and not is_now_in_campaign_schedule(horario):
                        logger.info(f"⏸️ Campaña {campaign_name} fuera de horario ({horario}), no se marcarán números pendientes.")
                        numbers = []
                    else:
                        # Incluir números con estado 'pendiente' Y números que pueden ser re-intentados
                        query = text(f"""
                            SELECT telefono FROM {campaign_name} 
                            WHERE (estado = 'pendiente' AND intentos < :max_intentos)
                            OR (estado IN ('N', 'U', 'E', 'R', 'I', 'X', 'T') AND intentos < :max_intentos)
                        """)
                        result = conn.execute(query, {"max_intentos": max_intentos})
                        numbers = [row[0] for row in result]
                        
                        logger.info(f"📋 Campaña {campaign_name}: {len(numbers)} números disponibles para marcar")

                destino = "9999"
                cps = cps_per_campaign

                logger.info(f"Iniciando dialer para campaña {campaign_name} con {len(numbers)} números (máx intentos: {max_intentos}, cps: {cps})")
                
                # 📊 Enviar estado inicial de la campaña
                if WEBSOCKET_AVAILABLE:
                    await safe_send_to_websocket(send_event_to_websocket, "campaign_started", {
                        "campaign_name": campaign_name,
                        "total_numbers": len(numbers),
                        "max_intentos": max_intentos,
                        "cps": cps
                    })
                
                dialer_tasks.append(
                    asyncio.create_task(
                        send_all_calls_persistent(numbers, cps, destino, campaign_name, max_intentos, stats)
                    )
                )

            if dialer_tasks:
                await asyncio.gather(*dialer_tasks)

            # Actualizar stats y arrays por campaña después de terminar los dialers
            for campaign_name in campaigns_data:
                stats = campaigns_data[campaign_name]["stats_obj"]
                campaigns_data[campaign_name]["stats"] = stats.to_dict()
                campaigns_data[campaign_name]["stats"]["campaign_name"] = campaign_name
                campaigns_data[campaign_name]["stats"]["timestamp"] = datetime.now().isoformat()
                campaigns_data[campaign_name]["ringing_numbers"] = list(getattr(stats, "ringing_numbers", []))
                campaigns_data[campaign_name]["active_numbers"] = list(getattr(stats, "active_numbers", []))
                campaigns_data[campaign_name]["answered_numbers"] = list(getattr(stats, "unique_answered", []))
                campaigns_data[campaign_name]["failed_numbers"] = list(getattr(stats, "unique_failed", []))
                campaigns_data[campaign_name]["busy_numbers"] = list(getattr(stats, "unique_busy", []))
                campaigns_data[campaign_name]["no_answer_numbers"] = list(getattr(stats, "unique_no_answer", []))

            # Enviar stats y arrays de todas las campañas activas al WebSocket
            if campaigns_data and WEBSOCKET_AVAILABLE:
                await safe_send_to_websocket(send_stats_to_websocket, {
                    "type": "multi_campaign_stats",
                    "data": [
                        {
                            **campaigns_data[c]["stats"],
                            "ringing_numbers": campaigns_data[c]["ringing_numbers"],
                            "active_numbers": campaigns_data[c]["active_numbers"],
                            "answered_numbers": campaigns_data[c]["answered_numbers"],
                            "failed_numbers": campaigns_data[c]["failed_numbers"],
                            "busy_numbers": campaigns_data[c]["busy_numbers"],
                            "no_answer_numbers": campaigns_data[c]["no_answer_numbers"],
                        }
                        for c in campaigns_data
                    ]
                })

            await asyncio.sleep(5)  # Más frecuente para updates más rápidos
            
        except Exception as e:
            logger.error(f"❌ Error en bucle principal: {e}")
            # Mantener WebSocket activo incluso si hay errores
            if WEBSOCKET_AVAILABLE:
                await safe_send_to_websocket(send_event_to_websocket, "system_error", {
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Monitor detenido por el usuario")