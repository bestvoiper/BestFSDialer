# 📋 Resumen de la API de Discador FreeSWITCH

## 🎯 **Propósito**
API desarrollada con FastAPI para gestionar campañas de marcación automática utilizando FreeSWITCH. Permite realizar llamadas masivas con control de CPS (Calls Per Second) y monitoreo en tiempo real.

## 🔗 **Arquitectura**
- **Backend**: Python con FastAPI
- **Base de Datos**: MySQL (tabla `bases` para campañas, tablas dinámicas para números)
- **Motor de Telefonía**: FreeSWITCH con conexión ESL
- **ORM**: SQLAlchemy

## 📡 **Endpoints Principales**

### 1. **Gestión de Campañas**
```http
GET /campanas/           # Lista todas las campañas
GET /campana/{nombre}    # Obtiene detalles de una campaña
```

### 2. **Control de Marcación**
```http
POST /discador/iniciar/  # Inicia el discador para una campaña
GET /discador/estado/{nombre}  # Obtiene estadísticas de la campaña
```

## ⚙️ **Funcionamiento**

### **1. Configuración de Campaña**
- Cada campaña tiene su propia tabla en la BD
- Configuración de parámetros: CPS, reintentos, detección AMD, horarios
- Estados de números: PENDIENTE, CONTESTADO, COMPLETADO, ERROR

### **2. Proceso de Marcación**
1. **Validación**: Verifica que la campaña existe y tiene números pendientes
2. **Conexión ESL**: Establece conexión persistente con FreeSWITCH
3. **Envío de Llamadas**: Ejecuta comandos `originate` con control de CPS
4. **Monitoreo**: Escucha eventos de FreeSWITCH en tiempo real

### **3. Monitoreo de Eventos**
```python
# Eventos monitoreados:
- CHANNEL_CREATE    # Llamada iniciada
- CHANNEL_ANSWER    # Llamada contestada
- CHANNEL_HANGUP    # Llamada terminada
```

### **4. Actualización de Estados**
- **CONTESTADO**: Cuando se detecta respuesta humana
- **COMPLETADO**: Llamada finalizada exitosamente
- **ERROR**: Falla en la llamada (ocupado, no contesta, etc.)

## 🔄 **Flujo de Trabajo**

1. **Inicialización**:
   ```bash
   curl -X POST /discador/iniciar/ \
   -d '{"nombre_campana": "PRUEBA", "cps": 5}'
   ```

2. **Ejecución**:
   - El sistema marca números según el CPS configurado
   - Actualiza estados en tiempo real
   - Registra estadísticas de llamadas

3. **Monitoreo**:
   ```bash
   curl /discador/estado/PRUEBA
   ```
   Retorna:
   ```json
   {
     "estadisticas": {
       "total": 100,
       "completados": 25,
       "pendientes": 50,
       "errores": 25
     }
   }
   ```

## 🌟 **Características Avanzadas**

### **1. Detección AMD (Answer Machine Detection)**
- Configuración por campaña
- Ruta diferente para buzones de voz vs. humanos

### **2. Control de CPS**
- Precisión en el envío de llamadas
- Evita saturación del proveedor

### **3. Sistema de Reintentos**
- Configuración personalizada por campaña
- Solo reintentos en números no exitosos

### **4. Monitoreo en Tiempo Real**
- Estadísticas actualizadas dinámicamente
- Logs detallados de eventos

## 🔧 **Configuración**

### **Conexión FreeSWITCH**
```python
FREESWITCH_HOST = "127.0.0.1"
FREESWITCH_PORT = 8021
FREESWITCH_PASSWORD = "1Pl}0F~~801l"
GATEWAY = "gw_pstn"
```

### **Base de Datos**
```python
DATABASE_URL = "mysql+pymysql://consultas:consultas@localhost/autodialer"
```

## 📊 **Ventajas**

1. **Escalabilidad**: Maneja múltiples campañas simultáneamente
2. **Precisión**: Control exacto de CPS
3. **Flexibilidad**: Configuración personalizada por campaña
4. **Monitoreo**: Estadísticas en tiempo real
5. **Robustez**: Manejo de errores y recuperación automática

## 🚀 **Uso Típico**

1. **Preparar campaña**: Cargar números en tabla específica
2. **Configurar parámetros**: CPS, reintentos, detección AMD
3. **Iniciar discador**: POST a `/discador/iniciar/`
4. **Monitorear progreso**: GET a `/discador/estado/`
5. **Analizar resultados**: Consultar estadísticas finales

## 🔮 **Casos de Uso**

- **Call Centers**: Campañas masivas de telemarketing
- **Encuestas**: Marcación automática para estudios
- **Cobros**: Llamadas automáticas de recordatorio
- **Emergencias**: Notificaciones masivas

## 🛠️ **Mantenimiento**

- **Logs estructurados**: Para debugging y auditoría
- **Documentación automática**: Swagger UI en `/docs`
- **Tests unitarios**: Validación de funcionalidad
- **Monitoreo de salud**: Endpoint `/health`

Esta API proporciona una solución completa para la gestión de llamadas automatizadas, combinando la potencia de FreeSWITCH con una interfaz moderna y fácil de usar.