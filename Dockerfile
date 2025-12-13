# Usar una imagen base de Ubuntu
FROM ubuntu:22.04

# Evitar interacciones durante la instalación
ENV DEBIAN_FRONTEND=noninteractive

# Actualizar el sistema e instalar Python, Git y herramientas útiles
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    curl \
    wget \
    vim \
    nano \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Crear enlaces simbólicos para python y pip (forzar si ya existen)
RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Configurar Git (estos valores pueden ser sobrescritos al ejecutar el contenedor)
RUN git config --global user.name "carbonecar" && \
    git config --global user.email "carbonecar@gmail.com" && \
    git config --global init.defaultBranch main

# Crear directorio de trabajo
WORKDIR /workspace

# Establecer variables de entorno con rutas absolutas
ENV AIRFLOW_HOME=/workspace/airflow_home
ENV DBT_PROFILES_DIR=/workspace/profiles
ENV DUCKDB_PATH=/workspace/warehouse/medallion.duckdb
ENV AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH=/workspace
ENV OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Configurar PATH para incluir el venv (se activará después de instalarlo)
ENV VIRTUAL_ENV=/workspace/.venv
ENV PATH="/workspace/.venv/bin:$PATH"

# Configurar bash como shell por defecto
SHELL ["/bin/bash", "-c"]

# Script de inicialización que se ejecutará al iniciar el contenedor
RUN echo '#!/bin/bash' > /entrypoint.sh && \
    echo 'set -e' >> /entrypoint.sh && \
    echo 'cd /workspace' >> /entrypoint.sh && \
    echo '' >> /entrypoint.sh && \
    echo '# Configurar entorno virtual si no existe' >> /entrypoint.sh && \
    echo 'if [ ! -d "/workspace/.venv" ]; then' >> /entrypoint.sh && \
    echo '  echo "=== Configurando entorno virtual ==="' >> /entrypoint.sh && \
    echo '  python -m venv .venv' >> /entrypoint.sh && \
    echo '  source .venv/bin/activate' >> /entrypoint.sh && \
    echo '  pip install --upgrade pip' >> /entrypoint.sh && \
    echo '  pip install -r requirements.txt' >> /entrypoint.sh && \
    echo '  echo "=== Entorno configurado correctamente ==="' >> /entrypoint.sh && \
    echo 'else' >> /entrypoint.sh && \
    echo '  echo "=== Entorno virtual ya existe ==="' >> /entrypoint.sh && \
    echo 'fi' >> /entrypoint.sh && \
    echo '' >> /entrypoint.sh && \
    echo '# Activar entorno virtual' >> /entrypoint.sh && \
    echo 'source /workspace/.venv/bin/activate' >> /entrypoint.sh && \
    echo '' >> /entrypoint.sh && \
    echo '# Ejecutar comando' >> /entrypoint.sh && \
    echo 'exec "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Configurar activación automática del entorno virtual en bash
RUN echo 'cd /workspace' >> /root/.bashrc && \
    echo '' >> /root/.bashrc && \
    echo '# Activar entorno virtual' >> /root/.bashrc && \
    echo 'if [ -f /workspace/.venv/bin/activate ]; then' >> /root/.bashrc && \
    echo '  source /workspace/.venv/bin/activate' >> /root/.bashrc && \
    echo 'fi' >> /root/.bashrc


# Exponer puerto 8080
EXPOSE 8080

# Usar el script de entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Comando por defecto
CMD ["/bin/bash"]
