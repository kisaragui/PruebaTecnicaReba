# Reba Challenge
Repositorio con las propuestas para el challenge de Reba.

Cada carpeta correnponde a la consigna indicada del documento pasado.
Dichas carpetas poseen un docker-compose.yml para ser ejecutas por separado para un mejor funcionamiento.

A continuacion se explicaran los requisitos faltantes para su correcto funciomiento 

Carpeta Punto1:

  1. Crear una carpeta llamada "data", contendra como salida los archivos a usar posteriormente.

Carpeta Punto2:

  1. Crear las carpetas "config", "logs", "plugins" para el entorno de Airflow y la carpeta "data" adentro de la carpeta "dags"
      para que ubicar los archivos posteriores.

 Luego iniciar el comando construir la imagen:

```bash
# Windows
docker-compose build

# Linux
sudo docker compose build
```

Una vez realiazado todo, correr el siguientes comando dependiendo de la carpeta:

```bash
#Carpeta Punto1:
# Windows
docker-compose up

# Linux
sudo docker compose up

#Carpeta Punto2:
# Windows
docker-compose up irflow-init

# Linux
sudo docker compose up airflow-init
```

Y luego para levantar todos los servicios: 

```bash
# Windows
docker-compose up

# Linux
sudo docker compose up
```


