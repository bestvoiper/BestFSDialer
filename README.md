# 🚀 DIALER BESTVOIPER

## 📋 Detalles
Las Carpetas son los codigos fuentes de cada app.

## 📦 Carpetas
- Python 3.8 o superior
- PostgreSQL
- FreeSWITCH con ESL habilitado
- Docker (opcional)

## 🔧 Instalación

1. Crear y activar entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
.\venv\Scripts\activate  # Windows
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

3. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

4. Iniciar la aplicación:
```bash
uvicorn app.main:app --reload
```

## 📚 Documentación de la API

### Endpoints Principales

#### 1. Realizar Llamada
```http
POST /api/v1/llamadas/
```
**Body:**
```json
{
    "numero": "123456789",
    "contexto": "default",
    "extension": "1000"
}
```

#### 2. Obtener Estado de Llamada
```http
GET /api/v1/llamadas/{llamada_id}
```

#### 3. Listar Llamadas
```http
GET /api/v1/llamadas/
```

#### 4. Cancelar Llamada
```http
DELETE /api/v1/llamadas/{llamada_id}
```

### Estados de Llamada
- `INICIADA`: Llamada iniciada
- `EN_PROGRESO`: Llamada en curso
- `COMPLETADA`: Llamada completada exitosamente
- `FALLIDA`: Llamada fallida
- `CANCELADA`: Llamada cancelada

## 🔍 Monitoreo de Eventos
La API monitorea los siguientes eventos de FreeSWITCH:
- `CHANNEL_CREATE`
- `CHANNEL_ANSWER`
- `CHANNEL_HANGUP`
- `CHANNEL_HANGUP_COMPLETE`

## ⚙️ Configuración
Las variables de entorno principales son:
```env
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
FREESWITCH_HOST=localhost
FREESWITCH_PORT=8021
FREESWITCH_PASSWORD=ClueCon
```

## 🧪 Pruebas
```bash
# Ejecutar pruebas
pytest

# Ejecutar pruebas con cobertura
pytest --cov=app tests/
```

## 📊 Ejemplo de Uso

### Realizar una Llamada
```python
import requests

response = requests.post(
    "http://localhost:8000/api/v1/llamadas/",
    json={
        "numero": "123456789",
        "contexto": "default",
        "extension": "1000"
    }
)
print(response.json())
```

### Monitorear Estado
```python
llamada_id = response.json()["id"]
estado = requests.get(f"http://localhost:8000/api/v1/llamadas/{llamada_id}")
print(estado.json())
```

## 🤝 Contribución
1. Fork el repositorio
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir un Pull Request

## 📝 Licencia
Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

## 👥 Autores
- Tu Nombre - [@tutwitter](https://twitter.com/tutwitter)

## 🙏 Agradecimientos
- FreeSWITCH
- FastAPI
- SQLAlchemy
- Comunidad de Python 