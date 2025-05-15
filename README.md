# 🚀 DIALER BESTVOIPER

## 📋 Detalles
Las Carpetas son los codigos fuentes de cada app.

## 📦 Carpetas
### AMD
```bash
Archivos de los scripts para detectar los buzon de voz y realizar el corte.
Quedaron como servicios
=> # systemctl status monitor_voicemail_killer
=> # systemctl status vosk-amd
```

Llamando a la extension 9110 va a detectar si es buzon, corta la llamada. Si detecta humano lo envia a otro dialplan.

### API

```bash
Archivos de configuración para poder conectar el front
```
la url http://emanuel.bestvoiper.com:8009/docs tiene los endpoints. Para poder utilizar.

