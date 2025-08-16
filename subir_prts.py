import subprocess
import sys
import signal
import os
import time
import threading
import socket
import logging
from datetime import datetime

PID_FILE = "./recordings/eralyws_pids.txt"
NAME_PREFIX = "eralyws_port_"
LOG_FILE = "./recordings/subir_prts.log"

# Usa los puertos del 8101 al 8120 inclusive
PORTS = list(range(8101, 8121))

# Configuración de monitoreo
MONITOR_INTERVAL = 30  # Segundos entre verificaciones
PORT_CHECK_TIMEOUT = 5  # Timeout para verificar puertos
MAX_RESTART_ATTEMPTS = 3  # Máximo intentos de reinicio por proceso

# Configuración de logging
def setup_logging():
    """Configurar el sistema de logging"""
    try:
        # Intentar crear directorio recordings en el directorio actual
        os.makedirs("./recordings", exist_ok=True)
        log_file = LOG_FILE
    except (PermissionError, OSError):
        # Si falla, usar /tmp como fallback
        log_file = f"/tmp/subir_prts_{os.getpid()}.log"
        print(f"Warning: Using fallback log file: {log_file}")
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    # Solo agregar FileHandler si podemos escribir el archivo
    try:
        handlers.append(logging.FileHandler(log_file))
    except (PermissionError, OSError) as e:
        print(f"Warning: Cannot write to log file {log_file}: {e}")
        print("Continuing with console logging only...")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def check_port_availability(port, retries=2):
    """Verificar si un puerto está activo y respondiendo con reintentos"""
    for attempt in range(retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(PORT_CHECK_TIMEOUT)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            if result == 0:
                return True
            if attempt < retries - 1:
                time.sleep(1)  # Esperar antes del siguiente intento
        except Exception as e:
            logging.debug(f"Error verificando puerto {port} (intento {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(1)
    return False

def is_process_running(pid):
    """Verificar si un proceso está ejecutándose"""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False

def get_safe_pid_file():
    """Obtener la ruta del archivo PID con manejo de permisos"""
    try:
        os.makedirs("./recordings", exist_ok=True)
        return PID_FILE
    except (PermissionError, OSError):
        # Si no podemos usar ./recordings, usar /tmp
        return f"/tmp/eralyws_pids_{os.getpid()}.txt"

def get_running_processes():
    """Obtener los procesos en ejecución desde el archivo PID"""
    processes = {}
    pid_file = get_safe_pid_file()
    
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(':')
                    if len(parts) == 2:
                        port = int(parts[0])
                        pid = int(parts[1])
                        if is_process_running(pid):
                            processes[port] = pid
                        else:
                            logging.debug(f"Proceso en puerto {port} (PID {pid}) no está ejecutándose")
        except Exception as e:
            logging.error(f"Error leyendo archivo PID: {e}")
    return processes


def start_process_for_port(port):
    """Iniciar un proceso para un puerto específico"""
    try:
        # Verificar que el archivo vosk_cli_args.py existe
        if not os.path.exists("vosk_cli_args.py"):
            logging.error("El archivo vosk_cli_args.py no existe en el directorio actual")
            return None
        
        cmd = ["python3", "vosk_cli_args.py", "--port", str(port)]
        env = os.environ.copy()
        env["ERALYWS_NAME"] = f"{NAME_PREFIX}{port}"
        env["WEBSOCKET_PORT"] = str(port)
        
        process = subprocess.Popen(
            cmd, 
            env=env,
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None
        )
        
        # Esperar un momento para verificar que el proceso no falle inmediatamente
        time.sleep(1)
        
        if process.poll() is None:
            logging.info(f"✅ Proceso iniciado para puerto {port} con PID {process.pid}")
            return process.pid
        else:
            stdout, stderr = process.communicate()
            logging.error(f"❌ El proceso para puerto {port} falló al iniciar: {stderr.decode()}")
            return None
            
    except Exception as e:
        logging.error(f"❌ Error iniciando proceso para puerto {port}: {e}")
        return None

def kill_process_by_pid(pid):
    """Terminar un proceso por su PID"""
    try:
        if is_process_running(pid):
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            if is_process_running(pid):
                os.kill(pid, signal.SIGKILL)
                time.sleep(1)
            logging.info(f"Proceso PID {pid} terminado")
            return True
    except Exception as e:
        logging.error(f"Error terminando proceso PID {pid}: {e}")
    return False

def update_pid_file(port, pid):
    """Actualizar el archivo PID con un nuevo proceso"""
    processes = {}
    pid_file = get_safe_pid_file()
    
    # Leer procesos existentes
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                for line in f:
                    parts = line.strip().split(':')
                    if len(parts) == 2:
                        p = int(parts[0])
                        pid_val = int(parts[1])
                        if p != port:  # No sobrescribir el puerto que estamos actualizando
                            processes[p] = pid_val
        except Exception as e:
            logging.error(f"Error leyendo archivo PID para actualización: {e}")
    
    # Agregar/actualizar el nuevo PID
    processes[port] = pid
    
    # Escribir archivo actualizado
    try:
        with open(pid_file, 'w') as f:
            for p, pid_val in sorted(processes.items()):
                f.write(f"{p}:{pid_val}\n")
    except Exception as e:
        logging.error(f"Error escribiendo archivo PID: {e}")

def remove_port_from_pid_file(port):
    """Remover un puerto del archivo PID"""
    pid_file = get_safe_pid_file()
    
    if not os.path.exists(pid_file):
        return
    
    processes = {}
    try:
        with open(pid_file, 'r') as f:
            for line in f:
                parts = line.strip().split(':')
                if len(parts) == 2:
                    p = int(parts[0])
                    pid_val = int(parts[1])
                    if p != port:
                        processes[p] = pid_val
        
        with open(pid_file, 'w') as f:
            for p, pid_val in sorted(processes.items()):
                f.write(f"{p}:{pid_val}\n")
    except Exception as e:
        logging.error(f"Error actualizando archivo PID: {e}")

class ProcessMonitor:
    """Monitor de procesos con reinicio automático"""
    
    def __init__(self):
        self.restart_counts = {port: 0 for port in PORTS}
        self.last_check_time = {port: 0 for port in PORTS}
        self.monitoring = False
        self.monitor_thread = None
    
    def start_monitoring(self):
        """Iniciar el monitoreo en un hilo separado"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logging.info("Monitor de procesos iniciado")
    
    def stop_monitoring(self):
        """Detener el monitoreo"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logging.info("Monitor de procesos detenido")
    
    def _monitor_loop(self):
        """Bucle principal de monitoreo"""
        while self.monitoring:
            try:
                self._check_and_restart_processes()
                time.sleep(MONITOR_INTERVAL)
            except Exception as e:
                logging.error(f"Error en bucle de monitoreo: {e}")
                time.sleep(MONITOR_INTERVAL)
    
    def _check_and_restart_processes(self):
        """Verificar y reiniciar procesos caídos"""
        current_time = time.time()
        running_processes = get_running_processes()
        
        for port in PORTS:
            process_alive = port in running_processes
            
            # Solo verificar puertos si ha pasado suficiente tiempo desde la última verificación
            time_since_last_check = current_time - self.last_check_time[port]
            if time_since_last_check < 10:  # Evitar verificaciones muy frecuentes
                continue
            
            port_active = check_port_availability(port, retries=3)
            self.last_check_time[port] = current_time
            
            needs_restart = False
            reason = ""
            
            if not process_alive:
                needs_restart = True
                reason = "proceso no encontrado"
            elif not port_active:
                needs_restart = True
                reason = "puerto no responde"
                # Terminar proceso zombie
                logging.warning(f"Puerto {port} tiene proceso (PID {running_processes[port]}) pero no responde")
                kill_process_by_pid(running_processes[port])
                remove_port_from_pid_file(port)
            
            if needs_restart:
                # Verificar límite de reintentos
                if self.restart_counts[port] >= MAX_RESTART_ATTEMPTS:
                    logging.error(f"🚫 Puerto {port} ha superado el límite de reintentos ({MAX_RESTART_ATTEMPTS}) - {reason}")
                    continue
                
                logging.warning(f"🔄 Reiniciando puerto {port} - {reason} (intento {self.restart_counts[port] + 1}/{MAX_RESTART_ATTEMPTS})")
                new_pid = start_process_for_port(port)
                
                if new_pid:
                    update_pid_file(port, new_pid)
                    self.restart_counts[port] += 1
                    
                    # Esperar y verificar que el puerto responda
                    time.sleep(8)  # Dar más tiempo para que el proceso inicie
                    if check_port_availability(port, retries=3):
                        logging.info(f"✅ Puerto {port} reiniciado exitosamente (PID {new_pid})")
                        # Reset contador de reintentos en caso de éxito
                        self.restart_counts[port] = 0
                    else:
                        logging.error(f"❌ El proceso reiniciado en puerto {port} no responde después de 8 segundos")
                else:
                    logging.error(f"❌ No se pudo reiniciar el proceso para puerto {port}")
                    self.restart_counts[port] += 1
            else:
                # Proceso funcionando correctamente
                if self.restart_counts[port] > 0:
                    # Solo resetear si había fallos previos
                    logging.debug(f"Puerto {port} funcionando correctamente - reseteando contador de reintentos")
                    self.restart_counts[port] = 0

    def get_status_summary(self):
        """Obtener un resumen del estado actual"""
        running_processes = get_running_processes()
        active_count = 0
        failed_count = 0
        
        for port in PORTS:
            process_alive = port in running_processes
            port_active = check_port_availability(port)
            
            if process_alive and port_active:
                active_count += 1
            else:
                failed_count += 1

        logging.info(f"Estado actual: {active_count} activos, {failed_count} con problemas")
        
        return active_count, failed_count


def start_ports():
    """Iniciar todos los procesos de puertos"""
    setup_logging()
    logging.info(f"🚀 Iniciando procesos en puertos: {PORTS}")
    
    processes = {}
    failed_ports = []
    
    for port in PORTS:
        logging.info(f"Iniciando proceso para puerto {port}...")
        pid = start_process_for_port(port)
        if pid:
            processes[port] = pid
        else:
            failed_ports.append(port)
            logging.error(f"❌ No se pudo iniciar proceso para puerto {port}")
    
    # Guardar PIDs en archivo
    pid_file = get_safe_pid_file()
    try:
        # Intentar crear directorio si usamos la ruta local
        if pid_file.startswith("./"):
            os.makedirs("./recordings", exist_ok=True)
        
        with open(pid_file, "w") as f:
            for port, pid in sorted(processes.items()):
                f.write(f"{port}:{pid}\n")
        logging.info(f"📝 PIDs guardados en: {pid_file}")
    except Exception as e:
        logging.error(f"❌ Error guardando PIDs: {e}")
    
    if failed_ports:
        logging.warning(f"⚠️ No se pudieron iniciar los puertos: {failed_ports}")
    
    logging.info(f"✅ Lanzados {len(processes)} de {len(PORTS)} procesos")
    
    # Verificar que los puertos respondan
    logging.info("🔍 Verificando conectividad de puertos...")
    time.sleep(5)  # Dar más tiempo para que todos los procesos inicien
    
    responsive_ports = []
    unresponsive_ports = []
    
    for port in processes.keys():
        if check_port_availability(port, retries=3):
            responsive_ports.append(port)
        else:
            unresponsive_ports.append(port)
    
    if unresponsive_ports:
        logging.warning(f"⚠️ Puertos que no responden: {unresponsive_ports}")
        logging.info("El monitor intentará reiniciarlos automáticamente")
    
    if responsive_ports:
        logging.info(f"✅ Puertos respondiendo correctamente: {responsive_ports}")
    
    return processes


def stop_ports():
    """Detener todos los procesos"""
    pid_file = get_safe_pid_file()
    
    if not os.path.exists(pid_file):
        logging.warning("⚠️ No se encontró el archivo de PIDs. No hay procesos para detener.")
        return
    
    stopped_count = 0
    try:
        with open(pid_file, "r") as f:
            for line in f:
                parts = line.strip().split(':')
                if len(parts) == 2:
                    port = int(parts[0])
                    pid = int(parts[1])
                    if kill_process_by_pid(pid):
                        stopped_count += 1
                        logging.info(f"🛑 Proceso puerto {port} (PID {pid}) detenido")
    except Exception as e:
        logging.error(f"Error deteniendo procesos: {e}")
    
    # Limpiar archivo PID
    try:
        os.remove(pid_file)
        logging.info(f"Archivo PID eliminado: {pid_file}")
    except Exception as e:
        logging.warning(f"No se pudo eliminar archivo PID: {e}")
    
    logging.info(f"🛑 {stopped_count} procesos detenidos")


def show_status():
    """Mostrar el estado actual de todos los procesos"""
    setup_logging()
    
    pid_file = get_safe_pid_file()
    if not os.path.exists(pid_file):
        logging.info("❌ No hay procesos ejecutándose (archivo PID no encontrado)")
        logging.info(f"   Buscando en: {pid_file}")
        return
    
    running_processes = get_running_processes()
    active_ports = []
    failed_ports = []
    
    logging.info("📊 Estado actual de los procesos:")
    logging.info("=" * 50)
    
    for port in PORTS:
        process_alive = port in running_processes
        port_active = check_port_availability(port)
        
        if process_alive and port_active:
            active_ports.append(port)
            status = "🟢 ACTIVO"
            pid = running_processes[port]
            logging.info(f"Puerto {port:4d}: {status} (PID {pid})")
        elif process_alive and not port_active:
            failed_ports.append(port)
            status = "🟡 PROCESO SIN RESPUESTA"
            pid = running_processes[port]
            logging.info(f"Puerto {port:4d}: {status} (PID {pid})")
        else:
            failed_ports.append(port)
            status = "🔴 INACTIVO"
            logging.info(f"Puerto {port:4d}: {status}")
    
    logging.info("=" * 50)
    logging.info(f"✅ Puertos activos: {len(active_ports)}/{len(PORTS)}")
    logging.info(f"❌ Puertos con problemas: {len(failed_ports)}")
    logging.info(f"📁 Archivo PID: {pid_file}")


def main():
    """Función principal"""
    setup_logging()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "bajar" or command == "stop":
            stop_ports()
            return
        
        elif command == "estado" or command == "status":
            show_status()
            return
        
        elif command == "monitor":
            # Modo solo monitoreo (sin iniciar procesos)
            logging.info("🔍 Iniciando en modo monitoreo...")
            monitor = ProcessMonitor()
            monitor.start_monitoring()
            
            try:
                status_report_interval = 300  # Reporte cada 5 minutos
                last_status_report = 0
                
                while True:
                    current_time = time.time()
                    if current_time - last_status_report >= status_report_interval:
                        active_count, failed_count = monitor.get_status_summary()
                        logging.info(f"📊 Estado: {active_count} activos, {failed_count} con problemas")
                        last_status_report = current_time
                    time.sleep(30)
            except KeyboardInterrupt:
                logging.info("🛑 Deteniendo monitoreo...")
                monitor.stop_monitoring()
            return
    
    # Modo normal: iniciar procesos y monitorear
    processes = start_ports()
    
    if not processes:
        logging.error("❌ No se pudo iniciar ningún proceso")
        sys.exit(1)
    
    # Iniciar monitoreo
    monitor = ProcessMonitor()
    monitor.start_monitoring()
    
    logging.info("ℹ️ Monitor automático activado - los puertos caídos se reiniciarán automáticamente")
    logging.info("ℹ️ Presiona Ctrl+C para detener todos los procesos")
    logging.info("ℹ️ Usa 'python3 subir_prts.py bajar' para detener desde otra terminal")
    logging.info("ℹ️ Usa 'python3 subir_prts.py estado' para ver el estado")
    logging.info("ℹ️ Usa 'python3 subir_prts.py monitor' para solo monitorear procesos existentes")
    
    try:
        status_report_interval = 300  # Reporte cada 5 minutos
        last_status_report = 0
        
        while True:
            current_time = time.time()
            if current_time - last_status_report >= status_report_interval:
                active_count, failed_count = monitor.get_status_summary()
                logging.info(f"📊 Estado del sistema: {active_count} puertos activos, {failed_count} con problemas")
                last_status_report = current_time
            time.sleep(30)
    except KeyboardInterrupt:
        logging.info("🛑 Deteniendo todos los procesos...")
        monitor.stop_monitoring()
        stop_ports()


if __name__ == "__main__":
    main()