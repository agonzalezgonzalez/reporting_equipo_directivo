# Reporting Equipo Directivo

Sistema automatizado de control de existencias, alertas de stock y dashboard interactivo.

## Estructura del proyecto

```
reporting_equipo_directivo/
├── data/
│   ├── raw/                          # Excels originales del ERP
│   └── processed/                    # Datos procesados e históricos
├── src/
│   ├── etl/
│   │   └── etl_existencias.py        # ETL: lectura y transformación del Excel
│   ├── alerts/
│   │   └── rules_existencias.py      # Motor de reglas de alerta (A1–A10)
│   └── utils/
│       ├── config_loader.py          # Carga de settings.yaml + .env
│       ├── email_sender.py           # Envío de emails SMTP
│       ├── logger.py                 # Logging centralizado
│       └── persistence.py            # Gestión de archivos históricos CSV
├── dashboard/
│   ├── app_main.py                   # Panel general (página principal)
│   └── pages/
│       ├── 1_Alertas_Activas.py
│       ├── 2_Detalle_Articulo.py
│       ├── 3_Evolucion.py
│       ├── 4_Proveedores.py
│       └── 5_Admin.py               # Panel de administración
├── config/
│   └── settings.yaml                 # Configuración central
├── .env.example                      # Plantilla de variables de entorno
├── requirements.txt
├── run_jobs.py                       # Orquestador principal
└── README.md
```

## Instalación

```bash
# 1. Crear entorno virtual
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
# .venv\Scripts\activate    # Windows

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar credenciales
cp .env.example .env
# Editar .env con las credenciales SMTP

# 4. Colocar el Excel del ERP
# Copiar EXISTENCIAS_MINIMO.xlsx a data/raw/
```

## Uso

### Ejecutar el ETL + Alertas
```bash
python run_jobs.py
```
Esto lee el Excel, evalúa las 10 reglas de alerta, genera los archivos históricos
y envía notificaciones por email si hay alertas activas.

### Lanzar el dashboard
```bash
cd dashboard
streamlit run app_main.py
```
Acceder desde el navegador a http://localhost:8501

### Automatizar con cron (Linux)
```bash
# Ejecutar cada día a las 8:00
0 8 * * * cd /ruta/proyecto && .venv/bin/python run_jobs.py >> logs/cron.log 2>&1
```

### Automatizar con Task Scheduler (Windows)
Crear tarea programada que ejecute:
```
C:\ruta\proyecto\.venv\Scripts\python.exe C:\ruta\proyecto\run_jobs.py
```

## Configuración

Toda la configuración está en `config/settings.yaml`:
- **Parámetros operativos**: umbrales de alertas, días de riesgo, rappels
- **Alertas**: activar/desactivar por tipo, canales de envío, acciones recomendadas
- **Destinatarios**: lista con nombre, email, teléfono, niveles y canales
- **Páginas**: activar/desactivar páginas del dashboard

También se puede gestionar desde el panel de Administración del dashboard (página ⚙️).
