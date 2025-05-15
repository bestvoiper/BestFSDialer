
# 🛡️ Vosk AMD + FreeSWITCH Voicemail Killer

Este proyecto implementa un sistema de detección automática de buzones de voz en llamadas salientes utilizando reconocimiento de voz en tiempo real con [Vosk](https://alphacephei.com/vosk/) y FreeSWITCH.

Cuando se detecta que una llamada fue atendida por un buzón de voz, la llamada se corta automáticamente mediante un monitor de eventos que escucha los eventos `uuid_setvar` desde FreeSWITCH.

---

## 📐 Arquitectura

```plaintext
Llamada saliente
      ↓
FreeSWITCH + mod_vosk
      ↓
WebSocket a Python Server (Vosk AMD)
      ↓
Detecta "buzón de voz"
      ↓
fs_cli -x "uuid_setvar current voicemail_is true"
      ↓
Monitor socket de eventos detecta uuid_setvar → uuid_kill
```

---

## 🧰 Requisitos

- FreeSWITCH compilado con [`mod_vosk`](https://github.com/alphacep/freeswitch/tree/master/src/mod/asr_tts/mod_vosk)
- Python 3.x
- Vosk Model (ej: `vosk-model-small-es-0.42`)
- Servicios habilitados: `mod_event_socket`, `mod_vosk`, `mod_dptools`
- Acceso a `fs_cli`

---

## 🗂️ Archivos principales

| Archivo | Descripción |
|--------|-------------|
| `improved_vosk_server4.py` | Servidor WebSocket que usa Vosk para detectar buzones de voz |
| `monitor_voicemail_killer.py` | Script que escucha eventos desde el socket de FreeSWITCH y corta llamadas si detecta `voicemail_is true` |
| `monitor_voicemail_killer.service` | Servicio systemd para correr el monitor en segundo plano |
| `vosk-amd.service` | Servicio systemd para correr el servidor WebSocket de detección |
| `README.md` | Esta documentación |

---

## 🔧 Configuración

### 1. Dialplan en FreeSWITCH

```xml
<extension name="analisis_voicemail">
  <condition field="destination_number" expression="^9110$">
    <action application="answer"/>
    <action application="sleep" data="1000"/>
    <action application="set" data="fire_asr_events=true"/>
    <action application="set" data="speech_websocket_uri=ws://127.0.0.1:2700"/>
    <action application="play_and_detect_speech" data="say:tts_commandline:espeak:'Diga algo por favor' detect:vosk default"/>
  </condition>
</extension>
```

> ❗ No se necesita lógica en Lua, toda la detección y acción se maneja por el servidor Python.

---

## ▶️ Cómo correrlo como servicio

### 1. Servicio del servidor Vosk (WebSocket)

**Archivo:** `/etc/systemd/system/vosk-amd.service`

```ini
[Unit]
Description=Vosk AMD Server
After=network.target

[Service]
ExecStart=/usr/bin/python3 /usr/local/freeswitch/scripts/improved_vosk_server4.py
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

### 2. Servicio del monitor de eventos de FreeSWITCH

**Archivo:** `/etc/systemd/system/monitor_voicemail_killer.service`

```ini
[Unit]
Description=FreeSWITCH Voicemail Killer Monitor
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/monitor_voicemail_killer.py
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

### Activación

```bash
sudo systemctl daemon-reexec
sudo systemctl enable vosk-amd monitor_voicemail_killer
sudo systemctl start vosk-amd monitor_voicemail_killer
```

---

## 🧪 Prueba de funcionamiento

1. Lanza una llamada a la extensión `9110` desde tu dialplan.
2. Si Vosk detecta una frase como `"la persona a la que ha llamado"`, se marcará como buzón.
3. El monitor ejecutará:

```bash
fs_cli -x "uuid_kill <UUID>"
```

4. La llamada será colgada automáticamente.

---

## ✅ Palabras clave detectadas

Estas son las frases consideradas indicativas de buzón de voz (pueden ampliarse):

```python
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
```

---

## 🛠️ Autor y Créditos

Creado por [Emanuel Ferreira](https://github.com/emanuelferreira), con amor por los sistemas VoIP, automatización y Python.

---

## 📄 Licencia

MIT License
