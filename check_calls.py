import asyncio
import time
import json
from datetime import datetime
from sqlalchemy import create_engine, text
import logging
import sys
import websockets
from vosk import Model, KaldiRecognizer
import subprocess

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
DB_URL = "mysql+pymysql://consultas:consultas@localhost/autodialer"

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

def update_call_status(campaign_name, numero, estado, engine, duracion="0", incrementar_intento=None, max_intentos=None):
    if not campaign_name:
        logger.error(f"Error actualizando estado: campaign_name vacío para número {numero}, estado {estado}")
        return
    try:
        with engine.begin() as conn:
            # Lógica para incrementar intentos:
            # - Si estado es 'P', siempre poner intentos=1 si es la primera vez (no sumar más de 1)
            # - Si estado es 'N', solo incrementar si intentos < max_intentos
            if estado == "P":
                # Solo poner intentos=1 si es la primera vez
                stmt = text(f"""
                    UPDATE {campaign_name}
                    SET estado = :estado,
                        duracion = :duracion,
                        intentos = CASE WHEN intentos < 1 THEN 1 ELSE intentos END
                    WHERE telefono = :numero
                """)
                conn.execute(stmt, {"estado": estado, "duracion": duracion, "numero": numero})
            elif estado == "N" and max_intentos is not None:
                # Solo incrementar si no supera el máximo
                intentos_actual = conn.execute(
                    text(f"SELECT intentos FROM {campaign_name} WHERE telefono = :numero"),
                    {"numero": numero}
                ).scalar() or 0
                if intentos_actual + 1 < max_intentos:
                    stmt = text(f"""
                        UPDATE {campaign_name}
                        SET estado = :estado,
                            duracion = :duracion,
                            intentos = intentos + 1
                        WHERE telefono = :numero
                    """)
                else:
                    stmt = text(f"""
                        UPDATE {campaign_name}
                        SET estado = :estado,
                            duracion = :duracion
                        WHERE telefono = :numero
                    """)
                conn.execute(stmt, {"estado": estado, "duracion": duracion, "numero": numero})
            else:
                # Otros estados no incrementan intentos
                stmt = text(f"""
                    UPDATE {campaign_name}
                    SET estado = :estado,
                        duracion = :duracion
                    WHERE telefono = :numero
                """)
                conn.execute(stmt, {"estado": estado, "duracion": duracion, "numero": numero})
    except Exception as e:
        logger.error(f"Error actualizando estado en campaña '{campaign_name}': {e}")

# Guarda el tiempo de respuesta por llamada contestada
answered_times = {}

# Palabras clave de buzón de voz
BUZON_KEYWORDS = [
    "no se encuentra disponible",
    "deje su mensaje",
    "la persona a la que ha llamado",
    "buzon de voz",
    "casilla de mensajes",
    "casilla de voz",
    "persona",
    "mensaje",
    "costo",
    "buzon",
    "grabar",
    "tono",
    "voz"
]

VOSK_MODEL_PATH = "/usr/local/freeswitch/grammar/model"  # Ajusta la ruta a tu modelo
vosk_model = None
try:
    vosk_model = Model(VOSK_MODEL_PATH)
except Exception as e:
    logger.warning(f"No se pudo cargar el modelo Vosk: {e}")

def detect_amd_vosk_realtime(audio_stream_generator):
    """
    Procesa audio PCM en tiempo real usando Vosk y detecta si es humano o buzón.
    audio_stream_generator: un generador que produce bytes de audio PCM 8kHz mono.
    Retorna: "human" o "machine"
    """
    if not vosk_model:
        logger.warning("Modelo Vosk no cargado, AMD deshabilitado.")
        return "human"
    rec = KaldiRecognizer(vosk_model, 8000)
    is_machine = False
    found_keyword = None
    last_text = ""
    for chunk in audio_stream_generator:
        if not chunk:
            continue
        if rec.AcceptWaveform(chunk):
            result = rec.Result()
            text = json.loads(result).get("text", "").lower()
            last_text = text
            logger.info(f"VOSK FINAL: {text}")
            for keyword in BUZON_KEYWORDS:
                if keyword in text:
                    is_machine = True
                    found_keyword = keyword
                    logger.info(f"VOSK AMD: Palabra clave FINAL detectada: '{keyword}' en '{text}'")
                    break
            if is_machine:
                break
        else:
            partial = json.loads(rec.PartialResult()).get("partial", "").lower()
            if partial:
                logger.info(f"VOSK PARCIAL: {partial}")
            for palabra in BUZON_KEYWORDS:
                if palabra in partial:
                    is_machine = True
                    found_keyword = palabra
                    logger.info(f"VOSK AMD: Palabra clave PARCIAL detectada: '{palabra}' en '{partial}'")
                    break
            if is_machine:
                break
    # --- NUEVO: Si no se detecta buzón pero el texto está vacío, asume buzón ---
    if is_machine:
        logger.info(f"VOSK AMD: Detectado BUZÓN por palabra clave: '{found_keyword}'")
        return "machine"
    elif not last_text.strip():
        logger.info("VOSK AMD: No se detectó texto, se asume BUZÓN")
        return "machine"
    else:
        logger.info("VOSK AMD: No se detectó buzón, se asume HUMANO")
        return "human"

async def handle_events_inline(con, campaign_name, stats, engine, max_intentos):
    global answered_times
    try:
        event = con.recvEventTimed(100)
        if not event:
            return
        event_str = event.serialize()
        numero = extract_field(event_str, "Caller-Destination-Number")
        uuid = extract_field(event_str, "Unique-ID")

        if "CHANNEL_PROGRESS" in event_str or "CHANNEL_PROGRESS_MEDIA" in event_str:
            logger.info(f"🔔 Llamada sonando: {numero}")
            stats.ringing_numbers.add(numero)
            stats.active_numbers.add(numero)
            stats.print_live_stats(campaign_name)
            await safe_send_to_websocket(send_event_to_websocket, "call_ringing", {
                "campaign": campaign_name,
                "numero": numero,
                "stats": stats.to_dict()
            })
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())

        elif "CHANNEL_ANSWER" in event_str:
            logger.info(f"📞 Llamada contestada: {numero}")
            stats.calls_answered += 1
            stats.ringing_numbers.discard(numero)
            stats.active_numbers.add(numero)
            update_call_status(campaign_name, numero, "S", engine)
            answered_times[(campaign_name, numero)] = time.time()
            stats.print_live_stats(campaign_name)
            await safe_send_to_websocket(send_event_to_websocket, "call_answered", {
                "campaign": campaign_name,
                "numero": numero,
                "stats": stats.to_dict()
            })
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())

            # --- AMD Vosk en tiempo real ---
            # Aquí deberías obtener el audio PCM en tiempo real del canal contestado.
            # Solución: usa sox para leer el audio del canal grabado en tiempo real (requiere que FreeSWITCH grabe el canal a un archivo .wav)
            # Si tienes mod_sofia o mod_dptools, puedes usar record_session en el dialplan para grabar el canal a /tmp/{uuid}.wav

            def audio_stream_generator():
                import time
                import os
                import wave

                audio_path = f"/tmp/{uuid}.wav"
                waited = 0
                # Espera a que el archivo exista y tenga algo de tamaño
                while not os.path.exists(audio_path) or os.path.getsize(audio_path) < 8000:
                    time.sleep(0.1)
                    waited += 0.1
                    if waited > 5:
                        break
                if not os.path.exists(audio_path):
                    return
                try:
                    wf = wave.open(audio_path, "rb")
                    while True:
                        data = wf.readframes(4000)
                        if not data:
                            time.sleep(0.1)  # Espera a que se grabe más audio
                            continue
                        yield data
                        if wf.tell() >= wf.getnframes():
                            break
                    wf.close()
                except Exception as e:
                    logger.error(f"Error leyendo audio para AMD Vosk: {e}")
                    return

            amd_result = detect_amd_vosk_realtime(audio_stream_generator())
            if amd_result == "machine":
                logger.info(f"🤖 AMD: Buzón detectado para {numero} (uuid={uuid})")
                if uuid:
                    con.api(f"uuid_kill {uuid}")
            else:
                logger.info(f"🧑 AMD: Humano detectado para {numero} (uuid={uuid})")
                if uuid:
                    transfer_cmd = f"uuid_transfer {uuid} 9999 XML default"
                    response = con.api(transfer_cmd)
                    if "+OK" in response.getBody():
                        logger.info(f"✅ Llamada transferida a 9999@default para {numero}")
                    else:
                        logger.warning(f"❌ Fallo al transferir llamada {numero}: {response.getBody()}")
                else:
                    logger.warning(f"❌ No se pudo obtener UUID para transferir llamada contestada {numero}")

        elif "CHANNEL_HANGUP_COMPLETE" in event_str:
            duracion = extract_field(event_str, "variable_duration")
            causa = extract_field(event_str, "Hangup-Cause")
            stats.ringing_numbers.discard(numero)
            stats.active_numbers.discard(numero)

            try:
                with engine.connect() as conn_db:
                    query = text(f"SELECT estado, intentos FROM {campaign_name} WHERE telefono = :numero")
                    result = conn_db.execute(query, {"numero": numero}).fetchone()
                    estado_actual = result[0] if result else None
                    intentos_actual = result[1] if result else 0
            except Exception as e:
                logger.error(f"Error consultando estado/intentos previos de {numero}: {e}")
                estado_actual = None
                intentos_actual = 0

            if causa == "NORMAL_CLEARING":
                logger.info(f"✅ Llamada completada: {numero}, Duración: {duracion}")
                if estado_actual == "S" and (campaign_name, numero) in answered_times:
                    answered_at = answered_times.pop((campaign_name, numero))
                    real_duration = int(time.time() - answered_at)
                    update_call_status(campaign_name, numero, "C", engine, str(real_duration), incrementar_intento=False)
                else:
                    update_call_status(campaign_name, numero, "C", engine, incrementar_intento=False)
                await safe_send_to_websocket(send_event_to_websocket, "call_completed", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "duracion": str(real_duration) if estado_actual == "S" and 'real_duration' in locals() else duracion,
                    "stats": stats.to_dict()
                })
            elif causa in ["USER_BUSY", "CALL_REJECTED"]:
                logger.info(f"📵 Línea ocupada: {numero}")
                stats.calls_busy += 1  # Para compatibilidad, pero solo cuenta únicos en .to_dict()
                stats.unique_busy.add(numero)
                update_call_status(campaign_name, numero, "O", engine, incrementar_intento=False)
                await safe_send_to_websocket(send_event_to_websocket, "call_busy", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "causa": causa,
                    "stats": stats.to_dict()
                })
            elif causa in ["NO_ANSWER", "ORIGINATOR_CANCEL"]:
                logger.info(f"📴 Sin respuesta: {numero}")
                stats.calls_no_answer += 1  # Para compatibilidad, pero solo cuenta únicos en .to_dict()
                stats.unique_no_answer.add(numero)
                update_call_status(
                    campaign_name, numero, "N", engine,
                    incrementar_intento=None, max_intentos=max_intentos
                )
                await safe_send_to_websocket(send_event_to_websocket, "call_no_answer", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "causa": causa,
                    "stats": stats.to_dict()
                })
            elif causa == "NO_USER_RESPONSE":
                logger.info(f"📴 Sin respuesta del usuario: {numero}")
                stats.calls_no_answer += 1
                stats.unique_no_answer.add(numero)
                update_call_status(campaign_name, numero, "U", engine, incrementar_intento=False)
                await safe_send_to_websocket(send_event_to_websocket, "call_no_answer_user", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "causa": causa,
                    "stats": stats.to_dict()
                })
            else:
                logger.info(f"❌ Llamada fallida: {numero}, Causa: {causa}")
                stats.calls_failed += 1
                stats.unique_failed.add(numero)
                update_call_status(campaign_name, numero, "E", engine, incrementar_intento=False)
                await safe_send_to_websocket(send_event_to_websocket, "call_failed", {
                    "campaign": campaign_name,
                    "numero": numero,
                    "causa": causa,
                    "stats": stats.to_dict()
                })

            stats.print_live_stats(campaign_name)
            await safe_send_to_websocket(send_stats_to_websocket, stats.to_dict())

    except Exception as e:
        logger.error(f"Error en handle_events_inline: {e}")

# --- NUEVA FUNCIÓN PARA VINCULAR CON VOSK AMD ---
async def start_vosk_amd(uuid, numero, campaign_name):
    """
    Envía audio RTP a Vosk AMD server vía WebSocket y espera resultado.
    Cuando detecta humano, transfiere la llamada; si es máquina, cuelga o marca variable.
    """
    vosk_ws_url = "ws://127.0.0.1:2700"  # Cambia si tu server Vosk está en otro host/puerto
    try:
        # Conecta al WebSocket de Vosk
        async with websockets.connect(vosk_ws_url) as ws:
            logger.info(f"🔊 Enviando audio de llamada {numero} (uuid={uuid}) a Vosk AMD...")
            # Aquí deberías enviar el audio RTP de la llamada a Vosk.
            # Esto depende de tu integración FreeSWITCH: puedes usar mod_rtc, mod_sofia, o grabar y reenviar el audio.
            # Por simplicidad, aquí solo escuchamos el resultado de Vosk.
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                if data.get("is_machine"):
                    logger.info(f"🤖 AMD: Buzón detectado para {numero} (uuid={uuid})")
                    # Puedes colgar la llamada o marcar variable en FreeSWITCH
                    # Ejemplo: colgar
                    import ESL
                    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
                    if con.connected():
                        con.api(f"uuid_kill {uuid}")
                    break
                elif "text" in data or data.get("confidence", 0) > 0.4:
                    logger.info(f"🧑 AMD: Humano detectado para {numero} (uuid={uuid})")
                    # Transferir la llamada a 9999
                    import ESL
                    con = ESL.ESLconnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD)
                    if con.connected():
                        con.api(f"uuid_transfer {uuid} 9999 XML default")
                    break
    except Exception as e:
        logger.error(f"Error en AMD Vosk para {numero} (uuid={uuid}): {e}")

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

    while i < total:
        batch = valid_numbers[i:i + cps]
        log(f"🚩 Enviando lote {i // cps + 1}: {len(batch)} llamadas")
        for numero in batch:
            uuid = f"{campaign_name}_{numero}"
            uuid_map[numero] = uuid
            originate_str = (
                f"bgapi originate "
                f"{{origination_caller_id_name='Outbound',ignore_early_media=true,"
                f"origination_uuid={uuid},"
                f"origination_caller_id_number='{numero}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} '&park()'\n\n"
            )
            con.api(originate_str)
            # Estado P: siempre poner intentos=1 si es la primera vez
            update_call_status(campaign_name, numero, "P", engine, incrementar_intento=None, max_intentos=max_intentos)
            stats.calls_sent += 1
            stats.unique_sent.add(numero)
            await safe_send_to_websocket(send_stats_to_websocket, {
                "campaign_name": campaign_name,
                "total_numbers": len(valid_numbers),
                "cps": cps,
                **stats.to_dict()
            })
            await asyncio.sleep(delay)
        for _ in range(cps * 3):
            await handle_events_inline(con, campaign_name, stats, engine, max_intentos)
        i += cps

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
        with engine.connect() as conn_db:
            query = text(f"SELECT COUNT(*) FROM {campaign_name} WHERE estado IN ('P', 'n')")
            pendientes = conn_db.execute(query).scalar()
        # 🔥 Actualiza stats.calls_answered con la BD para reflejar el valor real
        with engine.connect() as conn_db:
            query = text(f"SELECT COUNT(*) FROM {campaign_name} WHERE estado = 'S'")
            stats.calls_answered = conn_db.execute(query).scalar() or 0
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

    while True:
        with engine.connect() as conn:
            campaigns = conn.execute(text("SELECT base, reintentos FROM bases WHERE activo = 'S'"))
            active_campaigns = [(row[0], row[1]) for row in campaigns]

        # Limpiar campañas inactivas
        inactive = set(campaigns_data.keys()) - set(c for c, _ in active_campaigns)
        for c in inactive:
            del campaigns_data[c]

        num_active = len(active_campaigns)
        cps_global = 30
        cps_per_campaign = max(1, cps_global // num_active) if num_active > 0 else 1

        # --- NUEVO: lanzar todas las campañas en paralelo ---
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

            with engine.connect() as conn:
                query = text(f"""
                    SELECT telefono, intentos 
                    FROM {campaign_name} 
                    WHERE estado = 'n' 
                    AND intentos < :max_intentos
                    LIMIT 1000
                """)
                result = conn.execute(query, {"max_intentos": max_intentos})
                numbers = [row[0] for row in result]

            stats = campaigns_data[campaign_name]["stats_obj"]
            stats.campaign_name = campaign_name

            if not numbers:
                logger.info(f"No hay números pendientes para la campaña {campaign_name}")
                await safe_send_to_websocket(send_event_to_websocket, "campaign_idle", {
                    "campaign_name": campaign_name,
                    "message": "Campaña sin números pendientes"
                })
                campaigns_data[campaign_name]["stats"] = stats.to_dict()
                campaigns_data[campaign_name]["stats"]["campaign_name"] = campaign_name
                campaigns_data[campaign_name]["stats"]["total_numbers"] = 0
                campaigns_data[campaign_name]["stats"]["cps"] = 0
                campaigns_data[campaign_name]["stats"]["timestamp"] = datetime.now().isoformat()
                campaigns_data[campaign_name]["ringing_numbers"] = list(getattr(stats, "ringing_numbers", []))
                campaigns_data[campaign_name]["active_numbers"] = list(getattr(stats, "active_numbers", []))
                campaigns_data[campaign_name]["answered_numbers"] = list(getattr(stats, "unique_answered", []))
                campaigns_data[campaign_name]["failed_numbers"] = list(getattr(stats, "unique_failed", []))
                campaigns_data[campaign_name]["busy_numbers"] = list(getattr(stats, "unique_busy", []))
                campaigns_data[campaign_name]["no_answer_numbers"] = list(getattr(stats, "unique_no_answer", []))
                continue

            destino = "9999"
            cps = cps_per_campaign

            logger.info(f"Iniciando dialer para campaña {campaign_name} con {len(numbers)} números (máx intentos: {max_intentos}, cps: {cps})")
            # Lanzar cada campaña como tarea asíncrona
            dialer_tasks.append(
                asyncio.create_task(
                    send_all_calls_persistent(numbers, cps, destino, campaign_name, max_intentos, stats)
                )
            )

        # Esperar a que todas las campañas terminen su dialer antes de enviar stats globales
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
        if campaigns_data:
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

        await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Monitor detenido por el usuario")