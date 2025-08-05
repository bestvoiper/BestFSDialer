#!/bin/bash

# ================================================
# Script para lanzar instancias Vosk + NGINX auto
# ================================================

# USO:
#   ./setup_vosk_cluster.sh <n_instancias> <puerto_base> <max_sockets>
#   Ejemplo: ./setup_vosk_cluster.sh 20 8101 50

# =============================
# VALIDACIÓN DE PARÁMETROS
# =============================
if [ "$#" -ne 3 ]; then
    echo "Uso: $0 <n_instancias> <puerto_base> <max_sockets>"
    exit 1
fi

N_INSTANCIAS=$1
PUERTO_BASE=$2
MAX_SOCKETS=$3

SCRIPT="eralyws.py"
NGINX_CONFIG="/etc/nginx/sites-available/vosk"
NGINX_ENABLED="/etc/nginx/sites-enabled/vosk"

# =============================
# FUNCIONES
# =============================
launch_instances() {
    echo "▶️ Lanzando $N_INSTANCIAS instancias desde el puerto $PUERTO_BASE..."

    mkdir -p logs

    for ((i=0; i<N_INSTANCIAS; i++)); do
        PUERTO=$((PUERTO_BASE + i))
        echo " -> Iniciando en puerto $PUERTO..."
        nohup python3 "$SCRIPT" --port "$PUERTO" --max-sockets "$MAX_SOCKETS" > "logs/eralyws_$PUERTO.log" 2>&1 &
    done
}

generate_nginx_config() {
    echo "🛠 Generando configuración NGINX dinámica..."

    cat <<EOF > "$NGINX_CONFIG"
# Manejo de WebSockets
map \$http_upgrade \$connection_upgrade {
    default upgrade;
    ''      close;
}

# Cluster dinámico de Vosk
upstream vosk_cluster {
    least_conn;
EOF

    for ((i=0; i<N_INSTANCIAS; i++)); do
        PUERTO=$((PUERTO_BASE + i))
        echo "    server 127.0.0.1:$PUERTO;" >> "$NGINX_CONFIG"
    done

    cat <<EOF >> "$NGINX_CONFIG"
}

# Servidor NGINX en 8080
server {
    listen 8080;

    location / {
        proxy_pass http://vosk_cluster;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection \$connection_upgrade;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
EOF

    # Enlace simbólico si no existe
    if [ ! -L "$NGINX_ENABLED" ]; then
        ln -s "$NGINX_CONFIG" "$NGINX_ENABLED"
    fi

    echo "✅ Archivo NGINX generado en $NGINX_CONFIG"
}

reload_nginx() {
    echo "🔄 Verificando y recargando NGINX..."
    nginx -t && systemctl reload nginx
    if [ $? -eq 0 ]; then
        echo "✅ NGINX recargado exitosamente."
    else
        echo "❌ Error en configuración NGINX."
        exit 1
    fi
}

# =============================
# EJECUCIÓN DEL SCRIPT
# =============================
launch_instances
generate_nginx_config
reload_nginx

echo "🎉 Todo listo. Accede al servicio en: http://localhost:8080/"

