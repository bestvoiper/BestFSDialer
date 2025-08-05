
    #!/bin/bash

    # Script de inicio para API Discador
    WORKDIR="/var/www/discador_api"
    VENV_PATH="$WORKDIR/venv"
    LOGFILE="/var/log/discador_api.log"

    cd $WORKDIR

    # Activar entorno virtual
    source $VENV_PATH/bin/activate

    # Verificar que MySQL está corriendo
    if ! systemctl is-active --quiet mysql; then
        echo "MySQL no está corriendo. Iniciando..."
        sudo systemctl start mysql
    fi

    # Verificar que FreeSWITCH está corriendo
    if ! systemctl is-active --quiet freeswitch; then
        echo "FreeSWITCH no está corriendo. Iniciando..."
        sudo systemctl start freeswitch
    fi

    # Iniciar API
    echo "Iniciando API Discador..."
    uvicorn main:app --host 0.0.0.0 --port 8009 >> $LOGFILE 2>&1 &

    echo "API iniciada. PID: $!"
    echo "Logs disponibles en: $LOGFILE"