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
        self.campaign_name = campaign_name
        self.calls_sent = 0
        self.calls_answered = 0
        self.calls_failed = 0
        self.calls_busy = 0
        self.calls_no_answer = 0
        self.calls_ringing = 0
        self.ringing_numbers = set()  # set of (numero, uuid)
        self.active_numbers = set()   # set of (numero, uuid)
        self.unique_no_answer = set() # set of (numero, uuid)
        self.unique_failed = set()
        self.unique_busy = set()
        self.unique_answered = set()
        self.unique_sent = set()

    def print_live_stats(self, campaign_name=None):
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
            "ringing_numbers": [n for n, u in self.ringing_numbers],
            "active_calls": len(self.active_numbers),
            "active_numbers": [n for n, u in self.active_numbers],
            "total_processed": len(self.unique_answered) + len(self.unique_failed) + len(self.unique_busy) + len(self.unique_no_answer)
        }

def extract_field(event_str, field):
    try:
        return event_str.split(f"{field}: ")[1].split("\n")[0].strip()
    except:
        return ""

# Guarda el tiempo de respuesta por llamada contestada
answered_times = {}
# Guardar URLs de stream por UUID para iniciar cuando el canal esté listo
pending_streams = {}

async def handle_events_inline(con, campaign_name, stats, engine, max_intentos):
    global answered_times, pending_streams
    destino_transfer = "9999"
    try:
        event = con.recvEventTimed(100)
        if not event:
            return False
        event_str = event.serialize()
        numero = extract_field(event_str, "origination_caller_id_number")
        if not numero:
            numero = extract_field(event_str, "variable_callee_id_number")
        if not numero:
            numero = extract_field(event_str, "Caller-Destination-Number")
        if not numero:
            numero = extract_field(event_str, "Caller-Caller-ID-Number")
        uuid = extract_field(event_str, "Unique-ID")
        event_campaign = extract_field(event_str, "variable_campaign_name")
        key = (numero, uuid)

        if event_campaign and event_campaign != campaign_name:
            logger.debug(f"Ignorando evento de campaña diferente: {event_campaign} != {campaign_name}")
            return False

        if numero and uuid:
            try:
                with engine.connect() as conn_verify:
                    verify_query = text(f"SELECT COUNT(*) FROM `{campaign_name}` WHERE telefono = :numero OR uuid = :uuid")
                    count = conn_verify.execute(verify_query, {"numero": numero, "uuid": uuid}).scalar()
                    if count == 0:
                        logger.debug(f"Número {numero} no pertenece a campaña {campaign_name}, ignorando evento")
                        return False
            except Exception as e:
                logger.debug(f"Error verificando número en campaña: {e}")
                return False

        if numero == destino_transfer or not numero:
            logger.debug(f"Ignorando evento para número vacío o de transferencia: {numero}")
            return False

        if "CHANNEL_PROGRESS_MEDIA" in event_str or "CHANNEL_PROGRESS" in event_str:
            logger.info(f"🔔 [{campaign_name}] Llamada en progreso: {numero} ({uuid})")
            stats.ringing_numbers.add(key)
            stats.active_numbers.add(key)
            stats.print_live_stats(campaign_name)
            update_call_status(campaign_name, numero, "P", engine, uuid=uuid)
            await safe_send_to_websocket(send_event_to_websocket, "call_progress", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "status": "in_progress",
                "stats": stats.to_dict()
            })
            return True

        elif "CHANNEL_ANSWER" in event_str:
            logger.info(f"📞 [{campaign_name}] Llamada contestada: {numero} (UUID: {uuid})")
            stats.calls_answered += 1
            stats.unique_answered.add(key)
            stats.ringing_numbers.discard(key)
            stats.active_numbers.add(key)
            update_call_status(campaign_name, numero, "S", engine, uuid=uuid)
            answered_times[(campaign_name, uuid)] = time.time()
            stats.print_live_stats(campaign_name)
            await safe_send_to_websocket(send_event_to_websocket, "call_answered", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "stats": stats.to_dict()
            })
            async def transfer_call():
                try:
                    transfer_cmd = f"uuid_transfer {uuid} 9999 XML {campaign_name}"
                    logger.info(f"🔄 [{campaign_name}] Iniciando transferencia para {numero} (UUID: {uuid})")
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(None, lambda: con.api(transfer_cmd))
                    logger.info(f"🔄 [{campaign_name}] Respuesta de uuid_transfer para {numero} (UUID: {uuid}): {getattr(response, 'getBody', lambda: str(response))()}")
                    if response and "+OK" in response.getBody():
                        logger.info(f"✅ [{campaign_name}] Llamada transferida exitosamente: {numero} (UUID: {uuid})")
                        try:
                            with engine.begin() as conn_amd:
                                stmt = text(f"UPDATE `{campaign_name}` SET amd_result = 'HUMAN' WHERE uuid = :uuid")
                                result = conn_amd.execute(stmt, {"uuid": uuid})
                                if result.rowcount > 0:
                                    logger.info(f"✅ [{campaign_name}] AMD result actualizado a HUMAN para {numero} (UUID: {uuid})")
                        except Exception as e:
                            logger.error(f"❌ [{campaign_name}] Error actualizando amd_result: {e}")
                        stats.calls_answered -= 1
                        stats.active_numbers.discard(key)
                        await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())
                    else:
                        error_msg = response.getBody() if response else "Sin respuesta"
                        logger.warning(f"❌ [{campaign_name}] Fallo al transferir {numero} (UUID: {uuid}): {error_msg}")
                except Exception as e:
                    logger.error(f"❌ [{campaign_name}] Error en transferencia de {numero} (UUID: {uuid}): {e}")
            asyncio.create_task(transfer_call())
            return True

        elif "CHANNEL_HANGUP_COMPLETE" in event_str:
            duracion = extract_field(event_str, "variable_duration")
            causa = extract_field(event_str, "Hangup-Cause")
            stats.ringing_numbers.discard(key)
            stats.active_numbers.discard(key)
            try:
                with engine.connect() as conn_db:
                    query = text(f"SELECT estado, intentos FROM `{campaign_name}` WHERE uuid = :uuid")
                    result = conn_db.execute(query, {"uuid": uuid}).fetchone()
                    estado_actual = result[0] if result else None
                    intentos_actual = result[1] if result else 0
            except Exception as e:
                logger.error(f"Error consultando estado de {numero}: {e}")
                estado_actual = None
                intentos_actual = 0

            if causa == "NORMAL_CLEARING":
                logger.info(f"✅ [{campaign_name}] Llamada completada: {numero}, Duración: {duracion}")
                real_duration = None
                if estado_actual == "S":
                    if (campaign_name, uuid) in answered_times:
                        answered_at = answered_times.pop((campaign_name, uuid))
                        real_duration = int(time.time() - answered_at)
                    else:
                        logger.warning(f"[{campaign_name}] No se encontró answered_times para uuid {uuid} (numero {numero})")
                    update_call_status(
                        campaign_name, numero, "C", engine,
                        str(real_duration) if real_duration is not None else duracion,
                        incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason
                    )
                else:
                    update_call_status(campaign_name, numero, "C", engine, duracion, 
                                     incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
                stats.unique_answered.discard(key)
                await safe_send_to_websocket(send_event_to_websocket, "call_completed", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "uuid": uuid,
                    "duracion": str(real_duration) if real_duration is not None else duracion,
                    "hangup_reason": hangup_reason,
                    "stats": stats.to_dict()
                })
            elif causa in ["USER_BUSY", "CALL_REJECTED"]:
                logger.info(f"📵 [{campaign_name}] Línea ocupada: {numero}")
                stats.calls_busy += 1
                stats.unique_busy.add(key)
                update_call_status(campaign_name, numero, "O", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            elif causa in ["NO_ANSWER", "ORIGINATOR_CANCEL"]:
                logger.info(f"📴 [{campaign_name}] Sin respuesta: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(key)
                update_call_status(
                    campaign_name, numero, "N", engine,
                    incrementar_intento=None, max_intentos=max_intentos, uuid=uuid, hangup_reason=hangup_reason
                )
            elif causa == "NO_USER_RESPONSE":
                logger.info(f"📴 [{campaign_name}] Sin respuesta del usuario: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(key)
                update_call_status(campaign_name, numero, "U", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            elif causa == "NORMAL_TEMPORARY_FAILURE":
                logger.info(f"🚫 [{campaign_name}] Sin canales disponibles: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(key)
                update_call_status(campaign_name, numero, "E", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            elif causa == "NO_ROUTE_DESTINATION":
                logger.info(f"🗺️ [{campaign_name}] Sin ruta de destino: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(key)
                update_call_status(campaign_name, numero, "R", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            elif causa == "UNALLOCATED_NUMBER":
                logger.info(f"❌ [{campaign_name}] Número inexistente: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(key)
                update_call_status(campaign_name, numero, "I", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            elif causa == "INCOMPATIBLE_DESTINATION":
                logger.info(f"🔧 [{campaign_name}] Codecs incompatibles: {numero}")
                stats.calls_failed += 1
                stats.unique_failed.add(key)
                update_call_status(campaign_name, numero, "X", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            elif causa == "RECOVERY_ON_TIMER_EXPIRE":
                logger.info(f"⏰ [{campaign_name}] Timeout SIP: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(key)
                update_call_status(campaign_name, numero, "T", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            else:
                logger.info(f"❌ [{campaign_name}] Llamada fallida: {numero}, Causa: {causa}")
                stats.calls_failed += 1
                stats.unique_failed.add(key)
                update_call_status(campaign_name, numero, "E", engine, 
                                 incrementar_intento=False, uuid=uuid, hangup_reason=hangup_reason)
            stats.print_live_stats(campaign_name)
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())
            return True

        return False

    except Exception as e:
        logger.error(f"Error en handle_events_inline para {campaign_name}: {e}")
        return False

async def send_all_calls_persistent(numbers, cps, destino, campaign_name, max_intentos, stats=None, amd_type=None):
    if not 1 <= cps <= 100:
        raise ValueError(f"CPS fuera de rango permitido: {cps}")

    stats = stats or DialerStats(campaign_name=campaign_name)
    stats.campaign_name = campaign_name
    uuid_map = {}
    delay = 1 / cps
    engine = create_engine(DB_URL)
    log = logger.info

    log(f"🚦 [{campaign_name}] Enviando llamadas a {cps} CPS con AMD {amd_type or 'PRO'}")
    valid_numbers = [n for n in numbers if n.strip().isdigit()]
    log(f"📦 [{campaign_name}] {len(valid_numbers)} números válidos")

    # Verificar estado de campaña más robustamente
    if len(valid_numbers) == 0:
        log(f"📋 [{campaign_name}] Sin números pendientes para marcar")
        try:
            with engine.connect() as conn_check:
                # Verificar el estado real incluyendo llamadas transferidas
                campaign_status = conn_check.execute(text(f"""
                    SELECT 
                        COUNT(CASE WHEN estado IN ('P', 'S') THEN 1 END) as active_calls,
                        COUNT(CASE WHEN estado = 'C' AND amd_result = 'HUMAN' THEN 1 END) as transferred_calls,
                        COUNT(*) as total_calls,
                        COUNT(CASE WHEN estado IN ('C', 'E', 'O', 'N', 'U', 'R', 'I', 'X', 'T') THEN 1 END) as finalized_calls
                    FROM `{campaign_name}`
                """)).fetchone()
                
                active_calls = campaign_status[0] or 0
                transferred_calls = campaign_status[1] or 0
                total_calls = campaign_status[2] or 0
                finalized_calls = campaign_status[3] or 0  # <-- corregido nombre
                
                log(f"📊 [{campaign_name}] Estado: Total={total_calls}, Activas={active_calls}, Transferidas={transferred_calls}, Finalizadas={finalized_calls}")
                
                if active_calls > 0:
                    log(f"⏳ [{campaign_name}] Tiene {active_calls} llamadas activas - procesando en segundo plano")
                    stats.calls_answered = active_calls
                    stats.calls_sent = total_calls - finalized_calls if total_calls > finalized_calls else 0
                    
                    # Procesar eventos de llamadas activas por un tiempo
                    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
                    if con.connected():
                        con.events("plain", "CHANNEL_ANSWER CHANNEL_HANGUP_COMPLETE CHANNEL_PROGRESS")
                        log(f"✅ [{campaign_name}] Conectado a FreeSWITCH ESL para procesar llamadas activas")
                        
                        # Procesar eventos por 30 segundos máximo
                        for _ in range(300):  # 30 segundos / 0.1 segundos por iteración
                            event_processed = await handle_events_inline(con, campaign_name, stats, engine, max_intentos)
                            await asyncio.sleep(0.1)
                            
                            # Verificar si aún hay llamadas activas
                            current_active = conn_check.execute(text(f"""
                                SELECT COUNT(*) FROM `{campaign_name}` WHERE estado IN ('P', 'S')
                            """)).scalar() or 0
                            
                            if current_active == 0:
                                log(f"✅ [{campaign_name}] No hay más llamadas activas - finalizando procesamiento")
                                break
                        
                        con.disconnect()
                    
                    return stats
                    
                elif total_calls > 0 and finalized_calls >= total_calls and active_calls == 0:
                    log(f"🏁 [{campaign_name}] Completada - finalizando automáticamente")
                    try:
                        with engine.begin() as conn_finalize:
                            conn_finalize.execute(text("UPDATE campanas SET activo = 'F' WHERE nombre = :nombre"), 
                                                {"nombre": campaign_name})
                            log(f"✅ [{campaign_name}] Marcada como finalizada (F)")
                            
                            if WEBSOCKET_AVAILABLE:
                                await safe_send_to_websocket(send_event_to_websocket, "campaign_auto_finished", {
                                    "campaign_name": campaign_name,
                                    "message": "Campaña finalizada automáticamente - todas las llamadas completadas",
                                    "total_calls": total_calls,
                                    "finalized_calls": finalized_calls,
                                    "timestamp": datetime.now().isoformat()
                                })
                    except Exception as e:
                        logger.error(f"Error finalizando campaña {campaign_name}: {e}")
                    return stats
                    
        except Exception as e:
            logger.error(f"Error verificando estado de {campaign_name}: {e}")

    if WEBSOCKET_AVAILABLE:
        await safe_send_to_websocket(send_stats_to_websocket, {
            "campaign_name": campaign_name,
            "total_numbers": len(valid_numbers),
            "cps": cps,
            **stats.to_dict()
        })
        await asyncio.sleep(1)

    # Usar conexión ESL separada por campaña para evitar conflictos
    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
    if not con.connected():
        log(f"❌ [{campaign_name}] No se pudo conectar a FreeSWITCH ESL")
        return stats
    
    # Suscribirse solo a eventos específicos con filtro de campaña
    con.events("plain", "CHANNEL_ANSWER CHANNEL_HANGUP_COMPLETE CHANNEL_PROGRESS")
    log(f"✅ [{campaign_name}] Conectado a FreeSWITCH ESL")

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
            # Esperar hasta que no haya llamadas activas, sonando ni pendientes en BD
            pendientes = 0
            try:
                with engine.connect() as conn_db:
                    query = text(f"""
                        SELECT COUNT(*) FROM `{campaign_name}` 
                        WHERE (estado IN ('P', 'S', 'pendiente'))
                    """)
                    pendientes = conn_db.execute(query).scalar() or 0
            except Exception as e:
                logger.error(f"Error verificando pendientes en stats_updater: {e}")
                pendientes = 0
            if (stats.calls_sent >= len(valid_numbers) and 
                not stats.active_numbers and 
                not stats.ringing_numbers and 
                pendientes == 0):
                logger.info(f"📊 [{campaign_name}] Stats updater finalizando")
                break
            await asyncio.sleep(1)

    stats_task = asyncio.create_task(live_stats_updater())

    async def event_processor():
        events_without_data = 0
        max_idle_cycles = 50  # 5 segundos de espera máxima sin eventos
        while True:
            pendientes = 0
            try:
                with engine.connect() as conn_db:
                    query = text(f"""
                        SELECT COUNT(*) FROM `{campaign_name}` 
                        WHERE (estado IN ('P', 'S', 'pendiente'))
                    """)
                    pendientes = conn_db.execute(query).scalar() or 0
            except Exception as e:
                logger.error(f"Error verificando pendientes en event_processor: {e}")
                pendientes = 0
            if (stats.calls_sent < total or 
                stats.active_numbers or 
                stats.ringing_numbers or 
                pendientes > 0):
                event_processed = await handle_events_inline(con, campaign_name, stats, engine, max_intentos)
                if event_processed:
                    events_without_data = 0  # Reset contador si se procesó un evento
                else:
                    events_without_data += 1
                await asyncio.sleep(0.1)
                if (events_without_data >= max_idle_cycles and 
                    stats.calls_sent >= total and 
                    not stats.active_numbers and 
                    not stats.ringing_numbers and 
                    pendientes == 0):
                    logger.info(f"🔄 [{campaign_name}] Event processor finalizando - sin eventos por {events_without_data} ciclos")
                    break
            else:
                logger.info(f"🔄 [{campaign_name}] Event processor finalizando - todas las condiciones cumplidas")
                break
                
    event_task = asyncio.create_task(event_processor())

    # Enviar llamadas
    while i < total:
        # Verificar horario antes de cada lote
        try:
            with engine.connect() as conn_schedule:
                horario_result = conn_schedule.execute(text(f"SELECT horarios FROM campanas WHERE nombre = :nombre"), 
                                                     {"nombre": campaign_name}).fetchone()
                horario_actual = horario_result[0] if horario_result else None
                
                if horario_actual and not is_now_in_campaign_schedule(horario_actual):
                    log(f"⏸️ [{campaign_name}] Fuera de horario ({horario_actual}) - pausando")
                    try:
                        with engine.begin() as conn_update:
                            conn_update.execute(text("UPDATE campanas SET activo = 'P' WHERE nombre = :nombre"), 
                                              {"nombre": campaign_name})
                            log(f"✅ [{campaign_name}] Marcada como pausada (P)")
                    except Exception as e:
                        logger.error(f"Error pausando campaña {campaign_name}: {e}")
                    break
        except Exception as e:
            logger.error(f"Error verificando horario para {campaign_name}: {e}")
        
        batch = valid_numbers[i:i + cps]
        log(f"🚩 [{campaign_name}] Enviando lote {i // cps + 1}: {len(batch)} llamadas")
        
        for numero in batch:
            uuid = f"{campaign_name}_{numero}_{int(time.time()*1000)}"
            uuid_map[numero] = uuid
            
            # Configurar originate con variable de campaña
            if amd_type and amd_type.upper() == "FREE":
                originate_str = (
                    f"bgapi originate "
                    f"{{origination_caller_id_name='Outbound',ignore_early_media=false,"
                    f"origination_uuid={uuid},"
                    f"campaign_name='{campaign_name}',"
                    f"origination_caller_id_number='{numero}'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} &park()"
                )
            else:
                originate_str = (
                    f"bgapi originate "
                    f"{{origination_caller_id_name='Outbound',ignore_early_media=false,"
                    f"origination_uuid={uuid},"
                    f"campaign_name='{campaign_name}',"
                    f"origination_caller_id_number='{numero}'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} 2222 XML DETECT_AMD_PRO"
                )

            # Enviar originate de forma síncrona para evitar problemas con ESL
            try:
                response = con.api(originate_str)
                if response:
                    logger.debug(f"📞 [{campaign_name}] Originate enviado para {numero}: {response.getBody()}")
                else:
                    logger.warning(f"⚠️ [{campaign_name}] Sin respuesta al originate para {numero}")
            except Exception as e:
                logger.error(f"❌ [{campaign_name}] Error enviando llamada a {numero}: {e}")
                continue
            
            # Preparar URL del stream solo para AMD PRO
            if amd_type and amd_type.upper() != "FREE":
                wss_url_con_params = f"ws://localhost:8081/audio?uuid={uuid}&numero={numero}&campaign={campaign_name}"
                pending_streams[uuid] = wss_url_con_params
                logger.debug(f"📡 Stream preparado para {numero}: {wss_url_con_params}")
            
            # Insertar el uuid en la base de datos al iniciar la llamada y actualizar fecha_envio
            try:
                with engine.begin() as conn:
                    stmt = text(f"UPDATE `{campaign_name}` SET uuid = :uuid, fecha_envio = :fecha_envio WHERE telefono = :numero")
                    conn.execute(stmt, {
                        "uuid": uuid,
                        "numero": numero,
                        "fecha_envio": datetime.now()
                    })
                    logger.debug(f"📝 [{campaign_name}] UUID {uuid} guardado para {numero}")
            except Exception as e:
                logger.error(f"Error insertando uuid/fecha_envio para {numero} en {campaign_name}: {e}")
            
            update_call_status(campaign_name, numero, "pendiente", engine, 
                             incrementar_intento=None, max_intentos=max_intentos, uuid=uuid)
            stats.calls_sent += 1
            stats.unique_sent.add((numero, uuid))  # <-- corregido, antes era solo numero
            
            await safe_send_to_websocket(send_event_to_websocket, "call_pending", {
                "campaign": campaign_name,
                "numero": numero,
                "uuid": uuid,
                "status": "pending",
                "amd_type": amd_type or "PRO",
                "stats": stats.to_dict()
            })
            
            await asyncio.sleep(delay)
        i += cps

    # Esperar un tiempo adicional para procesar eventos de las llamadas enviadas
    log(f"⏳ [{campaign_name}] Esperando finalización de eventos...")
    await asyncio.sleep(2)  # Tiempo para que las llamadas se establezcan
    
    # Espera a que terminen los eventos pendientes
    await event_task
    await stats_task
    
    if len(valid_numbers) == 0:
        log(f"✅ [{campaign_name}] Sin números nuevos - finalizando")
        try:
            if con.connected():
                con.disconnect()
                log(f"✅ [{campaign_name}] Conexión ESL cerrada")
        except Exception as e:
            log(f"⚠️ [{campaign_name}] Error cerrando ESL: {e}")
        return stats
    
    # Esperar finalización con mejor verificación de estados
    max_wait = 60  # Reducir tiempo de espera
    waited = 0
    while waited < max_wait:
        # Verificar estados finales más robustamente
        pendientes = 0
        if valid_numbers:
            try:
                with engine.connect() as conn_db:
                    query = text(f"""
                        SELECT COUNT(*) FROM `{campaign_name}` 
                        WHERE telefono IN :nums 
                        AND (estado IN ('P', 'S') OR estado NOT IN ('C', 'E', 'O', 'N', 'U', 'R', 'I', 'X', 'T'))
                    """)
                    if len(valid_numbers) == 1:
                        pendientes = conn_db.execute(query, {"nums": (valid_numbers[0],)}).scalar() or 0
                    else:
                        pendientes = conn_db.execute(query, {"nums": tuple(valid_numbers)}).scalar() or 0
            except Exception as e:
                logger.error(f"Error verificando estados pendientes: {e}")
                pendientes = 0
        
        # Actualizar stats con datos reales de BD
        try:
            with engine.connect() as conn_db:
                real_stats = conn_db.execute(text(f"""
                    SELECT 
                        COUNT(CASE WHEN estado = 'S' THEN 1 END) as answered,
                        COUNT(CASE WHEN estado IN ('P', 'S') THEN 1 END) as active
                    FROM `{campaign_name}`
                """)).fetchone()
                stats.calls_answered = real_stats[0] or 0
                current_active = real_stats[1] or 0
        except Exception as e:
            logger.error(f"Error actualizando stats desde BD: {e}")
            current_active = 0
        
        await safe_send_to_websocket(send_stats_to_websocket, {
            "campaign_name": campaign_name,
            "total_numbers": len(valid_numbers),
            "cps": cps,
            **stats.to_dict()
        })
        
        if pendientes == 0 and not stats.active_numbers and not stats.ringing_numbers and current_active == 0:
            log(f"✅ [{campaign_name}] Todas las llamadas finalizadas")
            break
        
        # Procesar algunos eventos más
        for _ in range(5):
            await handle_events_inline(con, campaign_name, stats, engine, max_intentos)
            await asyncio.sleep(0.1)
        
        waited += 0.5

    # Cerrar conexión ESL de forma segura
    try:
        if con and con.connected():
            con.disconnect()
            log(f"✅ [{campaign_name}] Conexión ESL cerrada correctamente")
    except Exception as e:
        log(f"⚠️ [{campaign_name}] Error cerrando ESL: {e}")
    
    end = time.time()
    await safe_send_to_websocket(send_event_to_websocket, "campaign_finished", {
        "campaign_name": campaign_name,
        "total_sent": stats.calls_sent,
        "duration": end - start
    })
    
    log(f"\n✅ [{campaign_name}] {stats.calls_sent} llamadas enviadas en {end - start:.2f}s")
    return stats

def update_call_status(campaign_name, numero, estado, engine, duracion="0", incrementar_intento=None, max_intentos=None, uuid=None, hangup_reason=None):
    if not campaign_name:
        logger.error(f"Error actualizando estado: campaign_name vacío para número {numero}, estado {estado}")
        return
    try:
        with engine.begin() as conn:
            # Usar backticks para nombres de tabla que podrían ser palabras reservadas
            table_name = f"`{campaign_name}`"
            
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
                    UPDATE {table_name}
                    SET {update_fields},
                        uuid = :uuid
                    WHERE telefono = :numero
                """) if not uuid else text(f"""
                    UPDATE {table_name}
                    SET {update_fields}
                    WHERE uuid = :uuid
                """)
                result = conn.execute(stmt, {**params, "numero": numero, "uuid": uuid})
                logger.debug(f"📝 Estado '{estado}' actualizado para {numero} en {campaign_name} (filas afectadas: {result.rowcount})")
                
            elif estado == "N" and max_intentos is not None:
                intentos_actual = conn.execute(
                    text(f"SELECT intentos FROM {table_name} WHERE {where_clause}"),
                    params
                ).scalar() or 0
                
                update_fields = "estado = :estado, duracion = :duracion"
                if hangup_reason:
                    update_fields += ", hangup_reason = :hangup_reason"
                
                if intentos_actual + 1 < max_intentos:
                    update_fields += ", intentos = intentos + 1"
                
                stmt = text(f"""
                    UPDATE {table_name}
                    SET {update_fields}
                    WHERE {where_clause}
                """)
                result = conn.execute(stmt, params)
                logger.debug(f"📝 Estado '{estado}' actualizado para {numero} en {campaign_name} (filas afectadas: {result.rowcount})")
                
            else:
                update_fields = "estado = :estado, duracion = :duracion"
                if hangup_reason:
                    update_fields += ", hangup_reason = :hangup_reason"
                
                stmt = text(f"""
                    UPDATE {table_name}
                    SET {update_fields}
                    WHERE {where_clause}
                """)
                result = conn.execute(stmt, params)
                logger.debug(f"📝 Estado '{estado}' actualizado para {numero} en {campaign_name} (filas afectadas: {result.rowcount})")
                
    except Exception as e:
        logger.error(f"Error actualizando estado en campaña '{campaign_name}': {e}")

async def monitor_and_finalize_campaigns(engine):
    """Monitorea campanas para finalizarlas automáticamente cuando no tengan llamadas activas"""
    logger.info("🔍 Iniciando monitor de finalización automática de campanas")
    
    while True:
        try:
            # Obtener campanas activas que podrían necesitar finalización
            with engine.connect() as conn:
                active_campaigns = conn.execute(text("""
                    SELECT nombre FROM campanas 
                    WHERE activo = 'S' AND tipo = 'Audio'
                """)).fetchall()
                
                for row in active_campaigns:
                    campaign_name = row[0]
                    try:
                        # Verificar si la campaña tiene llamadas activas y números pendientes
                        stats = conn.execute(text(f"""
                            SELECT 
                                COUNT(CASE WHEN estado IN ('P', 'S') THEN 1 END) as active_calls,
                                COUNT(CASE WHEN estado = 'pendiente' OR (estado IN ('N', 'U', 'E', 'R', 'I', 'X', 'T') AND intentos < (SELECT reintentos FROM campanas WHERE nombre = :nombre)) THEN 1 END) as pending_numbers,
                                COUNT(*) as total_calls,
                                COUNT(CASE WHEN estado IN ('C', 'E', 'O', 'N', 'U', 'R', 'I', 'X', 'T') THEN 1 END) as completed_calls
                            FROM `{campaign_name}`
                        """), {"nombre": campaign_name}).fetchone()
                        
                        active_calls = stats[0] or 0
                        pending_numbers = stats[1] or 0
                        total_calls = stats[2] or 0
                        completed_calls = stats[3] or 0
                        
                        # Si no hay llamadas activas ni números pendientes, y hay llamadas completadas
                        if active_calls == 0 and pending_numbers == 0 and total_calls > 0:
                            logger.info(f"🏁 Finalizando automáticamente campaña {campaign_name} - sin llamadas activas ni números pendientes")
                            
                            try:
                                with engine.begin() as conn_finalize:
                                    conn_finalize.execute(text("UPDATE campanas SET activo = 'F' WHERE nombre = :nombre"), {"nombre": campaign_name})
                                    logger.info(f"✅ Campaña {campaign_name} marcada como finalizada (F) automáticamente")
                                
                                if WEBSOCKET_AVAILABLE:
                                    await safe_send_to_websocket(send_event_to_websocket, "campaign_auto_finished", {
                                        "campaign_name": campaign_name,
                                        "message": "Campaña finalizada automáticamente por monitor",
                                        "total_calls": total_calls,
                                        "completed_calls": completed_calls,
                                        "timestamp": datetime.now().isoformat()
                                    })
                            except Exception as e:
                                logger.error(f"Error finalizando automáticamente campaña {campaign_name}: {e}")
                        
                        elif active_calls == 0 and pending_numbers == 0 and total_calls == 0:
                            # Campaña completamente vacía
                            logger.info(f"🏁 Finalizando automáticamente campaña {campaign_name} - vacía")
                            try:
                                with engine.begin() as conn_finalize:
                                    conn_finalize.execute(text("UPDATE campanas SET activo = 'F' WHERE nombre = :nombre"), {"nombre": campaign_name})
                                    logger.info(f"✅ Campaña {campaign_name} marcada como finalizada (F) - vacía")
                            except Exception as e:
                                logger.error(f"Error finalizando campaña vacía {campaign_name}: {e}")
                                
                    except Exception as e:
                        logger.debug(f"Error monitoreando campaña {campaign_name}: {e}")
                
            await asyncio.sleep(10)  # Verificar cada 10 segundos
            
        except Exception as e:
            logger.error(f"❌ Error en monitor de finalización de campanas: {e}")
            await asyncio.sleep(10)

async def process_active_calls_background(engine):
    """Tarea en segundo plano para procesar eventos de llamadas activas de todas las campanas"""
    logger.info("🔄 Iniciando procesador de llamadas activas en segundo plano")
    
    while True:
        try:
            # Obtener todas las campanas que tienen llamadas activas
            with engine.connect() as conn:
                campaigns_with_active_calls = conn.execute(text("""
                    SELECT DISTINCT table_name as campaign_name
                    FROM information_schema.tables 
                    WHERE table_schema = 'masivos' 
                    AND table_name IN (
                        SELECT nombre FROM campanas WHERE tipo = 'Audio'
                    )
                """)).fetchall()
                
                active_campaigns = []
                for row in campaigns_with_active_calls:
                    campaign_name = row[0]
                    try:
                        # Verificar si tiene llamadas activas (P=sonando, S=contestada)
                        active_count = conn.execute(text(f"""
                            SELECT COUNT(*) FROM `{campaign_name}` 
                            WHERE estado IN ('P', 'S') AND uuid IS NOT NULL
                        """)).scalar() or 0
                        
                        if active_count > 0:
                            active_campaigns.append(campaign_name)
                            logger.debug(f"🔍 Campaña {campaign_name} tiene {active_count} llamadas activas")
                    except Exception as e:
                        logger.debug(f"Error verificando llamadas activas en {campaign_name}: {e}")
                
                if not active_campaigns:
                    # No hay llamadas activas, esperar más tiempo
                    await asyncio.sleep(5)
                    continue
                
                # Procesar eventos para cada campaña con llamadas activas
                for campaign_name in active_campaigns:
                    try:
                        # Conectar a FreeSWITCH ESL solo para esta campaña
                        con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
                        if con.connected():
                            con.events("plain", "CHANNEL_HANGUP_COMPLETE")
                            
                            # Crear stats temporales para esta campaña
                            temp_stats = DialerStats(campaign_name=campaign_name)
                            
                            # Obtener números activos de la BD y actualizar stats
                            active_numbers = conn.execute(text(f"""
                                SELECT telefono FROM `{campaign_name}` 
                                WHERE estado IN ('P', 'S') AND uuid IS NOT NULL
                            """)).fetchall()
                            
                            for row in active_numbers:
                                numero = row[0]
                                temp_stats.active_numbers.add(numero)
                                temp_stats.ringing_numbers.add(numero)
                            
                            # Procesar eventos por un tiempo corto
                            for _ in range(10):  # Procesar máximo 10 eventos por ciclo
                                await handle_events_inline(con, campaign_name, temp_stats, engine, 3)
                                await asyncio.sleep(0.01)
                            
                            con.disconnect()
                        
                    except Exception as e:
                        logger.debug(f"Error procesando eventos para {campaign_name}: {e}")
                
                await asyncio.sleep(1)  # Esperar 1 segundo antes del siguiente ciclo
                
        except Exception as e:
            logger.error(f"❌ Error en procesador de llamadas activas: {e}")
            await asyncio.sleep(5)

async def main():
    engine = create_engine(DB_URL)
    campaigns_data = {}
    
    try:
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
        
        # 🔄 Iniciar tareas en segundo plano
        background_task = asyncio.create_task(process_active_calls_background(engine))
        monitor_task = asyncio.create_task(monitor_and_finalize_campaigns(engine))
        logger.info("🎯 Iniciando bucle principal de monitoreo de campanas...")
        logger.info("🔄 Procesador de llamadas activas iniciado en segundo plano")
        logger.info("🔍 Monitor de finalización automática iniciado en segundo plano")

        while True:
            try:
                with engine.connect() as conn:
                    # Solo seleccionar campanas tipo 'Audio', fecha_programada válida y horario actual permitido
                    # INCLUIR campanas marcadas como 'F' (finalizadas) que hayan sido reactivadas a 'S'
                    campaigns = conn.execute(
                        text("""
                            SELECT nombre, reintentos, horarios 
                            FROM campanas 
                            WHERE activo = 'S' 
                            AND tipo = 'Audio'
                            AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                        """)
                    )
                    
                    # 🔄 Verificar campanas pausadas (P) para reactivarlas si entran en horario
                    paused_campaigns = conn.execute(
                        text("""
                            SELECT nombre, reintentos, horarios 
                            FROM campanas 
                            WHERE activo = 'P' 
                            AND tipo = 'Audio'
                            AND (fecha_programada IS NULL OR fecha_programada <= NOW())
                        """)
                    )
                    
                    # Procesar campanas pausadas
                    for row in paused_campaigns:
                        nombre, reintentos, horario = row[0], row[1], row[2] if len(row) > 2 else None
                        if is_now_in_campaign_schedule(horario):
                            logger.info(f"🔄 Campaña {nombre} pausada entrando en horario ({horario}) - reactivando")
                            try:
                                with engine.begin() as conn_reactivate:
                                    conn_reactivate.execute(text("UPDATE campanas SET activo = 'S' WHERE nombre = :nombre"), {"nombre": nombre})
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
                    
                    # Filtrar campanas por horario permitido
                    active_campaigns = []
                    for row in campaigns:
                        nombre, reintentos, horario = row[0], row[1], row[2] if len(row) > 2 else None
                        if is_now_in_campaign_schedule(horario):
                            active_campaigns.append((nombre, reintentos))
                        else:
                            logger.info(f"⏸️ Campaña {nombre} fuera de horario ({horario}), no se marcará en este ciclo.")

                # Limpiar campanas inactivas y finalizar campanas
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
                    logger.info("⏸️ No hay campanas activas. WebSocket permanece activo...")
                    # Enviar stats vacías para mantener la interfaz actualizada
                    if WEBSOCKET_AVAILABLE:
                        await safe_send_to_websocket(send_stats_to_websocket, {
                            "type": "multi_campaign_stats",
                            "data": [],
                            "message": "No hay campanas activas"
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

                    # Obtener números pendientes de la base y tipo de AMD
                    with engine.connect() as conn:
                        # --- NUEVO: Si la campaña tiene horario, solo marcar si está en horario ---
                        horario = None
                        amd_type = None
                        try:
                            res = conn.execute(text(f"SELECT horarios, amd FROM campanas WHERE nombre = :nombre"), {"nombre": campaign_name}).fetchone()
                            if res:
                                horario = res[0]
                                amd_type = res[1]  # Obtener tipo de AMD
                        except Exception:
                            pass
                        if horario and not is_now_in_campaign_schedule(horario):
                            logger.info(f"⏸️ Campaña {campaign_name} fuera de horario ({horario}), no se marcarán números pendientes.")
                            numbers = []
                        else:
                            # Incluir números con estado 'pendiente' Y números que pueden ser re-intentados
                            query = text(f"""
                                SELECT telefono FROM `{campaign_name}` 
                                WHERE (estado = 'pendiente' AND intentos < :max_intentos)
                                OR (estado IN ('N', 'U', 'E', 'R', 'I', 'X', 'T') AND intentos < :max_intentos)
                            """)
                            result = conn.execute(query, {"max_intentos": max_intentos})
                            numbers = [row[0] for row in result]
                            
                            logger.info(f"📋 Campaña {campaign_name}: {len(numbers)} números disponibles para marcar (AMD: {amd_type})")

                    destino = "9999"
                    cps = cps_per_campaign

                    logger.info(f"Iniciando dialer para campaña {campaign_name} con {len(numbers)} números (máx intentos: {max_intentos}, cps: {cps}, AMD: {amd_type or 'PRO'})")
                    
                    # 📊 Enviar estado inicial de la campaña
                    if WEBSOCKET_AVAILABLE:
                        await safe_send_to_websocket(send_event_to_websocket, "campaign_started", {
                            "campaign_name": campaign_name,
                            "total_numbers": len(numbers),
                            "max_intentos": max_intentos,
                            "cps": cps,
                            "amd_type": amd_type or "PRO"
                        })
                    
                    dialer_tasks.append(
                        asyncio.create_task(
                            send_all_calls_persistent(numbers, cps, destino, campaign_name, max_intentos, stats, amd_type)
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

                # Enviar stats y arrays de todas las campanas activas al WebSocket
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
                
    except Exception as e:
        logger.error(f"❌ Error crítico en main(): {e}")
        raise  # Re-lanzar la excepción para que systemd la detecte


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Monitor detenido por el usuario")
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}")
        sys.exit(1)  # Exit con código de error para systemd