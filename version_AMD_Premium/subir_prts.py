import subprocess
import sys
import signal
import os

PID_FILE = "./recordings/eralyws_pids.txt"
NAME_PREFIX = "eralyws_port_"

# Usa los puertos del 8101 al 8120 inclusive
PORTS = list(range(8101, 8121))


def start_ports():
    processes = []
    names = []
    print(f"Puertos seleccionados: {PORTS}")
    for port in PORTS:
        cmd = [
            "python3",
            "eralyws.py",
            "--port",
            str(port)
        ]
        # Añade nombre único al proceso usando el entorno
        env = os.environ.copy()
        env["ERALYWS_NAME"] = f"{NAME_PREFIX}{port}"
        proc = subprocess.Popen(cmd, env=env)
        processes.append(proc)
        names.append(f"{NAME_PREFIX}{port}")
    # Guarda los nombres en un archivo
    os.makedirs("./recordings", exist_ok=True)
    with open(PID_FILE, "w") as f:
        f.write("\n".join(names))
    print(f"✅ Lanzados {len(PORTS)} procesos en puertos {PORTS[0]}-{PORTS[-1]}")
    return processes


def stop_ports():
    if not os.path.exists(PID_FILE):
        print("⚠️ No se encontró el archivo de nombres. No hay procesos para detener.")
        return
    with open(PID_FILE, "r") as f:
        names = [line.strip() for line in f if line.strip()]
    # Buscar procesos por nombre y matarlos
    import psutil

    killed = 0
    for proc in psutil.process_iter(["pid", "name", "environ", "cmdline"]):
        try:
            env = proc.info.get("environ")
            if env:
                for name in names:
                    if env.get("ERALYWS_NAME") == name:
                        proc.kill()
                        print(f"🛑 Proceso {name} (pid {proc.pid}) detenido.")
                        killed += 1
                        break
        except Exception:
            pass
    os.remove(PID_FILE)
    print(f"🛑 {killed} procesos detenidos por nombre.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "bajar":
        try:
            import psutil
        except ImportError:
            print(
                "❌ psutil requerido para detener procesos por nombre. Instala con: pip install psutil"
            )
            sys.exit(1)
        stop_ports()
    else:
        start_ports()
        print(
            "ℹ️ Usa 'python3 subir_prts.py bajar' desde cualquier consola para detener todos los procesos por nombre."
        )
        try:
            signal.pause()
        except KeyboardInterrupt:
            try:
                import psutil
            except ImportError:
                print(
                    "❌ psutil requerido para detener procesos por nombre. Instala con: pip install psutil"
                )
                sys.exit(1)
            stop_ports()

# USO:
# Para subir: python3 subir_prts.py
# Para bajar: Ctrl+C en la terminal donde lo lanzaste (detiene todos los procesos lanzados por este script)
# Nota: Si ejecutas "python3 subir_prts.py bajar" en otra terminal, no podrá detener los procesos porque no tiene los objetos de los procesos lanzados.

# Explicación:
# - Cuando ejecutas el script normalmente, lanza los 60 procesos y espera indefinidamente.
# - Cuando presionas Ctrl+C, el script captura la señal y termina todos los procesos lanzados.
# - Si ejecutas con "bajar" como argumento, detiene los procesos usando los PIDs guardados en el archivo.

# Ejemplo:
# python3 subir_prts.py         # Lanza todos los procesos
# (Ctrl+C para detener todos los procesos)
# python3 subir_prts.py bajar   # Detiene todos los procesos lanzados