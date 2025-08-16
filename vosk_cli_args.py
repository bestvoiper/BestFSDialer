import asyncio
import websockets
import json
import os
import numpy as np
from vosk import Model, KaldiRecognizer
import urllib.parse
import csv
import ESL
import argparse
import sys
from sqlalchemy import create_engine, text

# === Argumentos CLI ===
DEFAULT_PORT = 8082
DEFAULT_MODEL_PATH = "./vosk-model-small-es-0.42"
DEFAULT_VERBOSE = False
DEFAULT_MAX_CONNECTIONS = 50

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Servidor WebSocket Vosk para detección de buzón de voz',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--port', '-p', type=int, default=DEFAULT_PORT, help='Puerto para el servidor WebSocket')
    parser.add_argument('--model-path', type=str, default=DEFAULT_MODEL_PATH, help='Ruta al modelo Vosk')
    parser.add_argument('--verbose', '-v', action='store_true', help='Mostrar información detallada de debugging')
    parser.add_argument('--max', '--max-connections', '-m', type=int, default=DEFAULT_MAX_CONNECTIONS, dest='max_connections', help='Máximo número de conexiones simultáneas')
    return parser.parse_args()

args = parse_arguments()
PORT = args.port
MODEL_PATH = args.model_path
VERBOSE = args.verbose
MAX_CONNECTIONS = args.max_connections if args.max_connections else DEFAULT_MAX_CONNECTIONS

SAMPLE_RATE = 16000
CSV_FILENAME = "./recordings/buzon_detectados.csv"
VOSK_GRAMMAR = [
    "disponible", "deje su mensaje", "numero", "suspendido", "buzon", "costo", "mensajes", "lo sentimos",
    "temporalmente", "ocupado", "mensaje", "contestador",
    "correo", "persona", "voz", "grabar", "marcado", "buzon", "sistema", "transferira", "su llamara", "se llamara", 
    "esta siendo", "despues", "después", "del tono", "momento", "despues del tono", "después del tono", "contestador",
    "contestadora", "telefonico","telefónica", "correo de voz"
]

# === Inicializar modelo Vosk ===
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"Modelo Vosk no encontrado en {MODEL_PATH}")
model = Model(MODEL_PATH)
if VERBOSE:
    print(f"✅ Modelo Vosk cargado desde {MODEL_PATH}")

# === Configuración de base de datos ===
DB_URL = "mysql+pymysql://consultas:consultas@localhost/masivos"

# === Guardar detección ===
def save_buzon_uuid(uuid, text, tipo="final"):
    file_exists = os.path.isfile(CSV_FILENAME)
    with open(CSV_FILENAME, "a", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["uuid", "type", "text"])
        writer.writerow([uuid, tipo, text])

def save_transcription_to_file(uuid, transcription, folder="./recordings"):
    """
    Guarda la transcripción completa en un archivo de texto.
    El archivo se llama <uuid>_transcripcion.txt en la carpeta indicada.
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    filepath = os.path.join(folder, f"{uuid}_transcripcion.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(transcription)

def save_transcription_no_grammar_to_file(uuid, transcription, folder="./recordings"):
    """
    Guarda la transcripción completa (sin grammar) en un archivo de texto.
    El archivo se llama <uuid>_transcripcion_nogrammar.txt en la carpeta indicada.
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    filepath = os.path.join(folder, f"{uuid}_transcripcion_nogrammar.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(transcription)

# === Colgar llamada ===
def update_amd_result(uuid, campaign_name, amd_result="MACHINE"):
    """
    Actualiza el campo amd_result en la base de datos para el UUID específico.
    """
    try:
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            stmt = text(f"UPDATE {campaign_name} SET amd_result = :amd_result, estado = 'C' WHERE uuid = :uuid")
            result = conn.execute(stmt, {"amd_result": amd_result, "uuid": uuid})
            if result.rowcount > 0:
                print(f"✅ AMD result actualizado para {uuid}: {amd_result}, estado: C")
            else:
                print(f"⚠️ No se encontró registro para UUID {uuid} en campaña {campaign_name}")
    except Exception as e:
        print(f"🚨 Error actualizando amd_result para {uuid}: {e}")

def hangup_call(uuid, campaign_name=None):
    try:
        # Actualizar amd_result en la base de datos si se proporciona campaign_name
        if campaign_name:
            update_amd_result(uuid, campaign_name, "MACHINE")
        
        # Colgar la llamada via ESL
        con = ESL.ESLconnection("127.0.0.1", 8021, "1Pl}0F~~801l")
        if con.connected():
            res = con.api("uuid_kill", uuid)
            print(f"🔴 Llamada colgada por ESL uuid={uuid}: {res.getBody()}")
        else:
            print(f"❌ Error: no conectado a FreeSWITCH ESL para uuid={uuid}")
    except Exception as e:
        print(f"🚨 Error al colgar uuid={uuid}: {e}")

# === Lógica por conexión WebSocket ===
active_connections = set()

async def handle_connection(websocket, path=None):
    connection_path = None
    uuid = None
    numero = None
    campaign = None
    
    try:
        # Intentar obtener el path de diferentes maneras según la versión de websockets
        if path is not None:
            # Versiones más antiguas pasan el path como parámetro
            connection_path = path
            print(f"🔗 Path obtenido como parámetro: {connection_path}")
        elif hasattr(websocket, 'path') and websocket.path:
            connection_path = websocket.path
            print(f"🔗 Path obtenido de websocket.path: {connection_path}")
        elif hasattr(websocket, 'request_uri') and websocket.request_uri:
            connection_path = websocket.request_uri
            print(f"🔗 Path obtenido de websocket.request_uri: {connection_path}")
        elif hasattr(websocket, 'uri') and websocket.uri:
            connection_path = websocket.uri
            print(f"🔗 Path obtenido de websocket.uri: {connection_path}")
        elif hasattr(websocket, 'request') and hasattr(websocket.request, 'path'):
            connection_path = websocket.request.path
            print(f"🔗 Path obtenido de websocket.request.path: {connection_path}")
        else:
            # Intentar obtener información de headers o raw_request_line
            if hasattr(websocket, 'request_uri'):
                connection_path = str(websocket.request_uri)
            elif hasattr(websocket, 'raw_request_line'):
                raw_line = str(websocket.raw_request_line)
                if 'GET ' in raw_line and ' HTTP' in raw_line:
                    connection_path = raw_line.split('GET ')[1].split(' HTTP')[0]
                    print(f"🔗 Path extraído de raw_request_line: {connection_path}")
            
            if not connection_path:
                print(f"❌ No se pudo obtener el path de la conexión WebSocket")
                print(f"📊 Atributos disponibles en websocket: {[attr for attr in dir(websocket) if not attr.startswith('_')]}")
                return
        
        if connection_path:
            # Extraer parámetros de la query string
            parsed_url = urllib.parse.urlparse(connection_path)
            query = dict(urllib.parse.parse_qsl(parsed_url.query))
            uuid = query.get("uuid")
            numero = query.get("numero")
            campaign = query.get("campaign")
            
            print(f"✅ Parámetros extraídos - UUID: {uuid}, Número: {numero}, Campaña: {campaign}")
            
            if not uuid or not numero:
                print(f"⚠️ Parámetros incompletos en la URL: {connection_path}")
                return
        else:
            print(f"❌ No se pudo obtener connection_path")
            return
            
    except Exception as e:
        print(f"🚨 Error obteniendo parámetros de conexión: {e}")
        print(f"📊 Tipo de websocket: {type(websocket)}")
        return

    print(f"✅ Nueva conexión: {numero} ({uuid}) de campaña {campaign}")

    if len(active_connections) >= MAX_CONNECTIONS:
        print(f"⚠️ Máximo de conexiones alcanzado ({MAX_CONNECTIONS}), rechazando nueva conexión")
        await websocket.close(code=1013, reason="Máximo de conexiones alcanzado")
        return
    active_connections.add(websocket)

    rec = KaldiRecognizer(model, SAMPLE_RATE, json.dumps(VOSK_GRAMMAR))
    rec.SetWords(True)

    try:
        async for message in websocket:
            if isinstance(message, bytes):
                audio = np.frombuffer(message, dtype=np.int16)
                detected = False

                if rec.AcceptWaveform(audio.tobytes()):
                    result = json.loads(rec.Result())
                    text = result.get('text', '').lower()
                    if VERBOSE:
                        print(f"🗣️ [{uuid}] Final: {text}")

                    # Guardar siempre en CSV (final)
                    save_buzon_uuid(uuid, text, "final")

                    if any(kw in text for kw in VOSK_GRAMMAR):
                        await websocket.send(json.dumps({"action": "colgar", "uuid": uuid, "text": text}))
                        hangup_call(uuid, campaign)
                        await asyncio.sleep(0.5)
                        try:
                            await websocket.close(code=1000, reason="Hangup detected")
                        except Exception as e:
                            print(f"🚨 Error cerrando WebSocket: {e}")
                        break
                    else:
                        await websocket.send(json.dumps({"type": "final", "uuid": uuid, "text": text}))
                else:
                    partial = json.loads(rec.PartialResult()).get("partial", "").lower()
                    if partial:
                        if VERBOSE:
                            print(f"⏳ [{uuid}] Parcial: {partial}")
                        # Guardar siempre en CSV (partial)
                        save_buzon_uuid(uuid, partial, "partial")
                        if any(kw in partial for kw in VOSK_GRAMMAR):
                            await websocket.send(json.dumps({"action": "colgar", "uuid": uuid, "text": partial}))
                            hangup_call(uuid, campaign)
                            await asyncio.sleep(0.5)
                            try:
                                await websocket.close(code=1000, reason="Hangup detected")
                            except Exception as e:
                                print(f"🚨 Error cerrando WebSocket: {e}")
                            break
            else:
                try:
                    msg = json.loads(message)
                    if msg.get("type") == "call_end":
                        print(f"🔚 Fin de llamada para {numero} [{uuid}]")
                        break
                except Exception:
                    pass
    except Exception as e:
        print(f"🚨 Error en la conexión [{uuid}]: {e}")
    finally:
        active_connections.discard(websocket)
        print(f"❌ Conexión cerrada [{len(active_connections)}/{MAX_CONNECTIONS}]: {uuid}")

# === Transcripción de audio completo sin usar grammar ===
def transcribe_full_audio_no_grammar(audio_bytes):
    """
    Transcribe todo el audio recibido usando Vosk sin grammar.
    Retorna el texto completo detectado.
    """
    rec = KaldiRecognizer(model, SAMPLE_RATE)
    rec.SetWords(True)
    rec.AcceptWaveform(audio_bytes)
    result = json.loads(rec.Result())
    return result.get('text', '').lower()

# === Servidor principal ===
async def main():
    print(f"🎧 Servidor WebSocket Vosk en ws://0.0.0.0:{PORT}/audio")
    async with websockets.serve(handle_connection, "0.0.0.0", PORT, ping_interval=None):
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Servidor detenido por el usuario")
    except Exception as e:
        print(f"\n🚨 Error crítico: {e}")
        sys.exit(1)