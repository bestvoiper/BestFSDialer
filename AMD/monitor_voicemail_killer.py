#!/usr/bin/env python3
import socket
import time
import subprocess

FS_HOST = "127.0.0.1"
FS_PORT = 8021
FS_PASSWORD = "1Pl}0F~~801l"

channel_map = {}

def send_cmd(sock, cmd):
    sock.send((cmd + "\n\n").encode())
    return sock.recv(4096).decode(errors='ignore')

def connect_and_auth():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((FS_HOST, FS_PORT))
    sock.recv(4096)
    sock.send(f"auth {FS_PASSWORD}\n\n".encode())
    response = sock.recv(4096).decode()
    if "-ERR" in response:
        print("❌ Autenticación fallida")
        sock.close()
        return None
    print("✅ Autenticado correctamente")
    return sock

def parse_event_block(event_str):
    lines = event_str.strip().splitlines()
    return {line.split(":", 1)[0].strip(): line.split(":", 1)[1].strip() for line in lines if ":" in line}

def uuid_kill(uuid):
    print(f"☠️ Ejecutando: uuid_kill {uuid}")
    cmd = f'fs_cli -p "{FS_PASSWORD}" -x "uuid_kill {uuid}"'
    subprocess.run(cmd, shell=True)

def main():
    sock = connect_and_auth()
    if not sock:
        return

    send_cmd(sock, "event plain all")
    buffer = ""

    try:
        while True:
            data = sock.recv(4096).decode(errors='ignore')
            buffer += data

            while "\n\n" in buffer:
                event_block, buffer = buffer.split("\n\n", 1)
                parsed = parse_event_block(event_block)
                event_name = parsed.get("Event-Name", "")
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

                if event_name == "CHANNEL_CREATE":
                    uuid = parsed.get("Unique-ID", "")
                    if uuid:
                        channel_map["current"] = uuid
                        print(f"[{timestamp}] 🧩 Mapeado 'current' -> {uuid}")

                elif event_name == "API":
                    arg = parsed.get("API-Command-Argument", "")
                    if "current%20voicemail_is%20true" in arg:
                        real_uuid = channel_map.get("current")
                        if real_uuid:
                            print(f"[{timestamp}] 📌 voicemail_is=true -> UUID real: {real_uuid}")
                            uuid_kill(real_uuid)
                        else:
                            print(f"[{timestamp}] ⚠️ UUID no mapeado para 'current'")
                    else:
                        print(f"\n🎯 [API EVENT DETECTADO - {timestamp}]")
                        print("-" * 50)
                        for k, v in parsed.items():
                            print(f"{k}: {v}")
                        print("-" * 50)

    except KeyboardInterrupt:
        print("🛑 Detenido por el usuario.")
        sock.close()

if __name__ == "__main__":
    main()

