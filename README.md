# 🌍 ETL Web Scraping — World Population Data

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Selenium](https://img.shields.io/badge/Selenium-4.x-43B02A?style=for-the-badge&logo=selenium&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-Dash-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-F7DF1E?style=for-the-badge)

**Pipeline ETL completo que extrae datos de población mundial desde Wikipedia,
los transforma con Pandas y los visualiza en un dashboard interactivo con Plotly Dash.**

[Ver Repositorio](https://github.com/jersonarrelucea14/ETL_Web_Scrapping) · [Reportar Bug](https://github.com/jersonarrelucea14/ETL_Web_Scrapping/issues) · [Solicitar Feature](https://github.com/jersonarrelucea14/ETL_Web_Scrapping/issues)

</div>

---

## 📋 Tabla de Contenidos

- [✨ Características](#-características)
- [🏗️ Arquitectura del Pipeline](#️-arquitectura-del-pipeline)
- [⚙️ Requisitos](#️-requisitos)
- [🚀 Instalación](#-instalación)
- [▶️ Uso](#️-uso)
- [🗄️ Estructura de Datos](#️-estructura-de-datos)
- [📊 Visualizaciones](#-visualizaciones)
- [📁 Estructura del Proyecto](#-estructura-del-proyecto)
- [💡 Ejemplos de Uso](#-ejemplos-de-uso)
- [📄 Licencia](#-licencia)
- [📬 Contacto](#-contacto)

---

## ✨ Características

- 🔍 **Web Scraping con Selenium** — Extracción automática desde Wikipedia con manejo de errores y rate limiting
- 🧹 **Transformación robusta** — Limpieza, normalización, validación y categorización de datos con Pandas
- 💾 **Exportación múltiple** — Genera archivos CSV, Excel (.xlsx) y JSON automáticamente
- 📊 **Dashboard interactivo** — Visualizaciones dinámicas con filtros en tiempo real usando Plotly Dash
- 📝 **Logging comprehensivo** — Registro detallado de cada fase del pipeline
- 🛡️ **Validación de seguridad** — Solo permite URLs de dominios autorizados (Wikipedia)
- 📐 **Métricas enriquecidas** — Categorización por tamaño, porcentaje acumulado, ranking relativo y más

---

## 🏗️ Arquitectura del Pipeline

```mermaid
graph LR
    A([🌐 Wikipedia]) -->|Selenium WebDriver| B

    subgraph EXTRACT ["📥 EXTRACT"]
        B[Validar URL]
        B --> C[Configurar Chrome Driver]
        C --> D[Cargar Página]
        D --> E[Extraer Tabla wikitable]
        E --> F[(Raw Data\nList of Dicts)]
    end

    subgraph TRANSFORM ["⚙️ TRANSFORM"]
        G[Limpiar Datos]
        G --> H[Convertir Tipos]
        H --> I[Validar Registros]
        I --> J[Enriquecer Datos]
        J --> K[Calcular Métricas]
        K --> L[(DataFrame\nPandas)]
    end

    subgraph LOAD ["📤 LOAD"]
        M[CSV]
        N[Excel .xlsx]
        O[JSON]
    end

    subgraph VISUALIZE ["📊 VISUALIZE"]
        P[Plotly Dash\nDashboard\nlocalhost:8050]
    end

    F -->|pandas.DataFrame| G
    L --> M
    L --> N
    L --> O
    M -->|pd.read_csv| P

    style EXTRACT fill:#dbeafe,stroke:#3b82f6,color:#1e3a5f
    style TRANSFORM fill:#dcfce7,stroke:#22c55e,color:#14532d
    style LOAD fill:#fef9c3,stroke:#eab308,color:#713f12
    style VISUALIZE fill:#f3e8ff,stroke:#a855f7,color:#3b0764
```

---

## ⚙️ Requisitos

| Requisito | Versión |
|---|---|
| Python | 3.10+ |
| Google Chrome | Última versión |
| Conexión a internet | Requerida para scraping |

---

## 🚀 Instalación

**1. Clona el repositorio:**

```bash
git clone https://github.com/jersonarrelucea14/ETL_Web_Scrapping.git
cd ETL_Web_Scrapping
```

**2. Crea un entorno virtual:**

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

**3. Instala las dependencias:**

```bash
pip install selenium pandas numpy openpyxl webdriver-manager plotly dash dash-bootstrap-components
```

> El `ChromeDriverManager` descargará automáticamente el driver compatible con tu versión de Chrome. No necesitas instalarlo manualmente.

---

## ▶️ Uso

### Ejecutar el pipeline ETL completo

```bash
python Webscrapping_Selenium_ETL.py
```

Verás la ejecución de las 3 fases en consola:

```
================================================================================
ETL PIPELINE - PAÍSES MÁS POBLADOS DEL MUNDO
================================================================================

FASE 1: EXTRACCIÓN
  ✔ WebDriver configurado
  ✔ 50 registros extraídos en 8.43s

FASE 2: TRANSFORMACIÓN
  ✔ Limpieza y normalización completada
  ✔ 50 registros válidos procesados en 0.12s

FASE 3: CARGA
  ✔ output/countries_population.csv
  ✔ output/countries_population.xlsx
  ✔ output/countries_population.json
```

### Lanzar el Dashboard

```bash
python world_population_dashboard.py
```

Luego abre tu navegador en **http://localhost:8050**

---

## 🗄️ Estructura de Datos

Los archivos generados en `/output` contienen las siguientes columnas:

| Columna | Tipo | Descripción |
|---|---|---|
| `country_clean` | `str` | Nombre del país normalizado |
| `population_numeric` | `int` | Población total (número entero) |
| `population_millions` | `float` | Población en millones |
| `percentage_numeric` | `float` | % de la población mundial |
| `cumulative_percentage` | `float` | Porcentaje acumulado |
| `population_category` | `category` | Pequeño / Mediano / Grande / Muy Grande / Mega Poblado |
| `relative_rank` | `float` | Ranking por densidad relativa |
| `population_diff_millions` | `float` | Diferencia respecto al país anterior (M) |
| `processed_at` | `str` | Fecha y hora de procesamiento |
| `extraction_year` | `int` | Año de extracción |

---

## 📊 Visualizaciones

El dashboard incluye 5 tipos de gráficos interactivos con filtros dinámicos:

**Vista principal — KPIs y gráficos de población**

![Dashboard Principal](images/imagen1.png)

**Vista secundaria — Treemap jerárquico y tabla de datos**

![Treemap y Tabla](images/imagen2.png)

### Gráficos disponibles

| Visualización | Descripción |
|---|---|
| 📊 Barras / Líneas | Top N países por población (switchable) |
| 🍩 Donut chart | Distribución porcentual global |
| 📈 Área acumulada | Crecimiento de población acumulada |
| 🗂️ Treemap | Mapa jerárquico agrupado por continente |
| 📋 Tabla detallada | País, población y % mundial |

### Filtros disponibles
- **Número de países** a mostrar: slider de 5 a 50
- **Filtro por continente**: Asia, América, Europa, África, Oceanía
- **Tipo de gráfico**: Barras o Líneas

---

## 📁 Estructura del Proyecto

```
ETL_Web_Scrapping/
│
├── 📄 Webscrapping_Selenium_ETL.py      # Pipeline principal ETL
├── 📄 world_population_dashboard.py     # Dashboard interactivo Plotly Dash
│
├── 📂 output/                           # Archivos generados por el ETL
│   ├── countries_population.csv
│   ├── countries_population.json
│   └── countries_population.xlsx
│
├── 📂 images/                           # Capturas del dashboard
│   ├── imagen1.png
│   └── imagen2.png
│
├── 📂 logs/                             # Logs de ejecución
│   └── etl_pipeline.log
│
├── 📄 .gitignore
└── 📄 README.md
```

---

## 💡 Ejemplos de Uso

### Uso básico — pipeline completo

```python
from Webscrapping_Selenium_ETL import WikipediaCountriesETL

url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
etl = WikipediaCountriesETL(url, max_records=50)

results = etl.run_full_pipeline(
    output_formats=['csv', 'excel', 'json'],
    output_filename='countries_population'
)
```

### Ejecutar fases por separado

```python
# Solo extracción
raw_data = etl.extract()

# Solo transformación
df = etl.transform()

# Exportar en un formato específico
etl.load(output_format='json', filename='mi_archivo')
```

### Consultar resultados

```python
# Top 10 países más poblados
print(etl.display_top_countries(10))

# Estadísticas descriptivas
print(etl.get_summary_statistics())

# Metadata del proceso ETL
metadata = etl.get_metadata()
print(f"Tiempo de extracción: {metadata['extraction_time']:.2f}s")
print(f"Registros válidos: {metadata['valid_records']}")
```

### Lanzar el dashboard desde código

```python
from world_population_dashboard import create_dashboard_from_file

dashboard = create_dashboard_from_file(
    'output/countries_population.csv',
    title="World Population Analytics Dashboard"
)
dashboard.create_app()
dashboard.run(debug=True, port=8050)
```

---

## 📄 Licencia

Distribuido bajo la licencia **MIT**. Consulta el archivo `LICENSE` para más detalles.

---

## 📬 Contacto

<div align="center">

**Jerson Arrelucea**

[![GitHub](https://img.shields.io/badge/GitHub-jersonarrelucea14-181717?style=for-the-badge&logo=github)](https://github.com/jersonarrelucea14)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Jerson_Arrelucea-0A66C2?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/jerson-arrelucea-ing)
[![Email](https://img.shields.io/badge/Email-jersonarrelucea14@gmail.com-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:jersonarrelucea14@gmail.com)

⭐ Si este proyecto te fue útil, ¡no olvides darle una estrella en GitHub!

</div>
