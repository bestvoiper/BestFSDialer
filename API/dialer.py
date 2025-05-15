import asyncio
import csv
import time
from datetime import datetime
import argparse
from tqdm.asyncio import tqdm
from sqlalchemy import create_engine, text
from database import engine

FREESWITCH_HOST = "127.0.0.1"
FREESWITCH_PORT = 8021
FREESWITCH_PASSWORD = "1Pl}0F~~801l"
GATEWAY = "gw_pstn"

class DialerStats:
    def __init__(self):
        self.calls_sent = 0
        self.calls_answered = 0
        self.calls_failed = 0
        self.active_calls = 0

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

class ESLConnection:
    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
        self.reader = None
        self.writer = None

    async def __aenter__(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            await self.reader.readuntil(b"auth/request\n")
            self.writer.write(f"auth {self.password}\n\n".encode())
            await self.writer.drain()
            await self.reader.readuntil(b"+OK accepted\n")
            return self
        except Exception as e:
            log(f"❌ Error conectando a ESL: {e}")
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def send_command(self, command):
        self.writer.write(command.encode())
        await self.writer.drain()
        response = await self.reader.readuntil(b"\n")
        return response.decode().strip()

    async def subscribe_events(self):
        await self.send_command("event plain ALL\n\n")

async def update_call_status(campaign_name, numero, estado, duracion="0"):
    """Actualiza el estado de una llamada en la base de datos"""
    try:
        query = text(f"""
            UPDATE {campaign_name}
            SET estado = :estado,
                duracion = :duracion,
                intentos = intentos + 1
            WHERE telefono = :numero
            AND estado = 'PENDIENTE'
        """)
        with engine.connect() as conn:
            conn.execute(query, {"estado": estado, "duracion": duracion, "numero": numero})
            conn.commit()
    except Exception as e:
        log(f"❌ Error actualizando estado en BD: {e}")

async def monitor_calls(campaign_name, stats):
    """Monitorea el estado de las llamadas en FreeSWITCH"""
    async with ESLConnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD) as conn:
        await conn.subscribe_events()
        
        while True:
            try:
                # Obtener información de llamadas activas
                response = await conn.send_command("show calls\n\n")
                stats.active_calls = response.count("total")

                # Obtener eventos de llamadas
                event = await conn.reader.readuntil(b"\n\n")
                event_str = event.decode()
                
                if "CHANNEL_ANSWER" in event_str:
                    stats.calls_answered += 1
                    # Extraer número y actualizar estado
                    numero = event_str.split("Caller-Caller-ID-Number: ")[1].split("\n")[0]
                    await update_call_status(campaign_name, numero, "CONTESTADO")
                
                elif "CHANNEL_HANGUP" in event_str:
                    # Extraer información relevante
                    numero = event_str.split("Caller-Caller-ID-Number: ")[1].split("\n")[0]
                    duracion = event_str.split("variable_duration: ")[1].split("\n")[0]
                    causa = event_str.split("Hangup-Cause: ")[1].split("\n")[0]
                    
                    if causa == "NORMAL_CLEARING":
                        await update_call_status(campaign_name, numero, "COMPLETADO", duracion)
                    else:
                        stats.calls_failed += 1
                        await update_call_status(campaign_name, numero, "ERROR", duracion)

            except Exception as e:
                log(f"❌ Error monitoreando llamadas: {e}")
                await asyncio.sleep(1)
                continue

async def incrementar_intento(campaign_name, numero):
    try:
        query = text(f"""
            UPDATE {campaign_name}
            SET intentos = intentos + 1
            WHERE telefono = :numero
        """)
        with engine.connect() as conn:
            conn.execute(query, {"numero": numero})
            conn.commit()
    except Exception as e:
        log(f"❌ Error actualizando intentos en BD: {e}")

async def send_all_calls_persistent(numbers, cps, destino, campaign_name):
    if not validate_cps(cps):
        raise ValueError(f"CPS debe estar entre 1 y 100. Valor recibido: {cps}")

    stats = DialerStats()
    delay = 1 / cps
    log(f"🚦 Enviando llamadas a {cps} CPS (~{delay:.3f}s entre originates)")

    valid_numbers = [num for num in numbers if validate_phone_number(num)]
    if len(valid_numbers) != len(numbers):
        log(f"⚠️ Se encontraron {len(numbers) - len(valid_numbers)} números inválidos")

    # Iniciar el monitor de llamadas en segundo plano
    monitor_task = asyncio.create_task(monitor_calls(campaign_name, stats))

    async with ESLConnection(FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD) as conn:
        log("✅ Autenticado correctamente")
        
        progress = tqdm(total=len(valid_numbers), desc="📞 Enviando llamadas")
        start = time.time()

        try:
            for numero in valid_numbers:
                uuid = f"uuid_{numero}_{int(time.time()*1000)}"
                originate_str = (
                    f"bgapi originate "
                    f"{{origination_caller_id_name='AMD Test',origination_caller_id_number='1000'}}"
                    f"sofia/gateway/{GATEWAY}/{numero} "
                    f"'&transfer({destino} XML default)'\n\n"
                )
                await conn.send_command(originate_str)
                await incrementar_intento(campaign_name, numero)
                await asyncio.sleep(delay)
                stats.calls_sent += 1
                progress.update(1)

                # Actualizar estadísticas en la base de datos
                log(f"📊 Estadísticas - Enviadas: {stats.calls_sent}, "
                    f"Contestadas: {stats.calls_answered}, "
                    f"Fallidas: {stats.calls_failed}, "
                    f"Activas: {stats.active_calls}")

        except Exception as e:
            log(f"❌ Error durante el envío de llamadas: {e}")
            raise
        finally:
            end = time.time()
            duration = end - start
            progress.close()
            
            if stats.calls_sent > 0:
                log(f"✅ {stats.calls_sent} llamadas enviadas en {duration:.2f} segundos")
                log(f"🚀 Promedio de envío: {stats.calls_sent / duration:.2f} CPS reales")

            # Cancelar el monitor de llamadas
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

def validate_phone_number(number):
    return number.strip().isdigit()

def validate_cps(cps):
    return 1 <= cps <= 100

def main():
    parser = argparse.ArgumentParser(description="Dialer con conexión persistente ESL + control CPS")
    parser.add_argument('--csv', type=str, required=True, help='Archivo CSV con los números')
    parser.add_argument('--cps', type=int, default=50, help='Llamadas por segundo (CPS)')
    parser.add_argument('--dst', type=str, default="9110", help='Destino en Dialplan')
    parser.add_argument('--campaign', type=str, required=True, help='Nombre de la campaña')
    args = parser.parse_args()

    try:
        with open(args.csv, newline='') as csvfile:
            reader = csv.reader(csvfile)
            numbers = [row[0] for row in reader if row]

        if not numbers:
            log("❌ El archivo CSV está vacío")
            return

        log(f"📦 Cargando {len(numbers)} números desde {args.csv}")
        asyncio.run(send_all_calls_persistent(numbers, args.cps, args.dst, args.campaign))

    except FileNotFoundError:
        log(f"❌ No se encontró el archivo CSV: {args.csv}")
    except Exception as e:
        log(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
