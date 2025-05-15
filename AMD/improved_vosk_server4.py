#!/usr/bin/env python3
import asyncio
import websockets
import json
import logging
import subprocess
from vosk import Model, KaldiRecognizer

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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='/var/log/vosk_amd.log'
)

class VoskRealtimeServer:
    def __init__(self, host='0.0.0.0', port=2700, model_path='/usr/local/freeswitch/grammar/model'):
        self.host = host
        self.port = port
        self.model_path = model_path
        self.model = Model(model_path)
        self.freeswitch_password = "1Pl}0F~~801l"

    def create_recognizer(self):
        return KaldiRecognizer(self.model, 8000)

    def set_freeswitch_variable(self, uuid, variable, value):
        try:
            cmd = f'fs_cli -p "{self.freeswitch_password}" -x "uuid_setvar {uuid} {variable} {value}"'
            subprocess.run(cmd, shell=True, check=True)
            logging.info(f"Variable {variable} establecida a {value} para llamada {uuid}")
        except Exception as e:
            logging.error(f"Error al establecer variable en FreeSWITCH: {e}")

    async def handle_audio_stream(self, websocket, path):
        recognizer = self.create_recognizer()
        client = websocket.remote_address
        logging.info(f"Conexion nueva desde {client}")

        try:
            async for message in websocket:
                if not isinstance(message, bytes):
                    continue

                if recognizer.AcceptWaveform(message):
                    result = json.loads(recognizer.Result())
                    text = result.get("text", "").lower()
                    
                    # Verificar si es buzón de voz
                    is_machine = False
                    for keyword in BUZON_KEYWORDS:
                        if keyword in text:
                            is_machine = True
                            break
                    
                    # Enviar resultado en el formato esperado
                    response = {
                        "text": text,
                        "is_machine": is_machine,
                        "confidence": 0.5
                    }
                    await websocket.send(json.dumps(response))
                    logging.info(f"FINAL: {text} (is_machine: {is_machine})")
                    
                    if is_machine:
                        self.set_freeswitch_variable("current", "voicemail_is", "true")
                        await websocket.close()
                        return
                else:
                    partial = json.loads(recognizer.PartialResult()).get("partial", "").lower()
                    if partial:
                        # Enviar resultado parcial en el formato esperado
                        response = {
                            "partial": json.dumps({
                                "partial": partial
                            })
                        }
                        await websocket.send(json.dumps(response))
                        logging.info(f"PARCIAL: {partial}")

                        for palabra in BUZON_KEYWORDS:
                            if palabra in partial:
                                logging.info(f"🚨 PALABRA CLAVE DETECTADA: '{palabra}' en '{partial}'")
                                self.set_freeswitch_variable("current", "voicemail_is", "true")
                                await websocket.close()
                                return

        except websockets.exceptions.ConnectionClosed:
            logging.info(f"Conexion cerrada con {client}")
        except Exception as e:
            logging.error(f"Error en la conexion con {client}: {e}")

    async def start_server(self):
        logging.info(f"Iniciando servidor Vosk en ws://{self.host}:{self.port}")
        async with websockets.serve(self.handle_audio_stream, self.host, self.port):
            await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(VoskRealtimeServer().start_server())

