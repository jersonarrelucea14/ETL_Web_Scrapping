"""
ETL Pipeline para extracción de datos de países desde Wikipedia.

Este módulo implementa un pipeline ETL completo con las siguientes características:
- Extracción web mediante Selenium
- Transformación y limpieza de datos con Pandas
- Validación robusta de datos
- Logging comprehensivo
- Manejo de errores
- Exportación en múltiples formatos

Autor: Jerson Arrelucea
"""

# ============================================================================
# IMPORTS 
# ============================================================================
import time
import logging
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import re

import world_population_dashboard as dashboard_module

# Data processing
import pandas as pd
import numpy as np

# Web scraping
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================================
def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura el sistema de logging para el pipeline.
    
    Args:
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Ruta opcional para guardar logs en archivo
        
    Returns:
        Logger configurado
    """
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        handlers.append(
            logging.FileHandler(log_dir / log_file, encoding='utf-8')
        )
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    return logging.getLogger('ETL_Pipeline')


logger = setup_logging(log_file='etl_pipeline.log')


# ============================================================================
# CLASE ETL PRINCIPAL
# ============================================================================
class WikipediaCountriesETL:
    """
    ETL Pipeline para extraer, transformar y cargar datos de países desde Wikipedia.

    Implementa el patrón ETL con las siguientes características:
    - Extracción mediante Selenium
    - Transformación con Pandas
    - Validación de datos
    - Manejo robusto de errores
    - Logging comprehensivo
    
    Attributes:
        url (str): URL de Wikipedia a extraer
        raw_data (List[Dict]): Datos crudos extraídos
        transformed_data (pd.DataFrame): Datos transformados
        metadata (Dict): Metadata del proceso ETL
    """

    # Configuración de seguridad
    ALLOWED_DOMAINS = ['wikipedia.org', 'en.wikipedia.org']
    DEFAULT_TIMEOUT = 30
    MAX_RECORDS = 50
    
    def __init__(self, url: str, max_records: int = MAX_RECORDS):
        """
        Inicializa el pipeline ETL.

        Args:
            url: URL de Wikipedia a extraer
            max_records: Número máximo de registros a extraer
        """
        self.url = url
        self.max_records = max_records
        self.driver = None
        self.raw_data = None
        self.transformed_data = None
        self.metadata = {
            'extraction_time': None,
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'transformation_time': None,
            'extraction_date': datetime.now().isoformat()
        }

    def _validate_url(self) -> None:
        """
        Valida la URL por seguridad.
        
        Raises:
            ValueError: Si la URL no es segura o no está permitida
        """
        if not self.url.startswith('https://'):
            raise ValueError("Solo se permiten URLs HTTPS por seguridad")

        from urllib.parse import urlparse
        domain = urlparse(self.url).netloc
        
        if not any(allowed in domain for allowed in self.ALLOWED_DOMAINS):
            raise ValueError(
                f"Dominio no permitido: {domain}. "
                f"Dominios permitidos: {', '.join(self.ALLOWED_DOMAINS)}"
            )

    def _setup_driver(self) -> webdriver.Chrome:
        """
        Configura el driver de Selenium con opciones optimizadas y seguras.

        Returns:
            WebDriver configurado
        """
        logger.info("Configurando Selenium WebDriver...")

        chrome_options = Options()
        
        # Opciones de rendimiento y seguridad
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        # User agent legítimo
        chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(self.DEFAULT_TIMEOUT)

        logger.info("WebDriver configurado exitosamente")
        return driver

    # ========================================================================
    # EXTRACT
    # ========================================================================
    def extract(self) -> List[Dict]:
        """
        Extrae datos de la tabla de Wikipedia usando Selenium.

        Returns:
            Lista de diccionarios con los datos extraídos
            
        Raises:
            ValueError: Si la URL no es válida
            TimeoutException: Si la página tarda demasiado en cargar
        """
        logger.info(f"Iniciando extracción desde: {self.url}")
        
        # Validación de URL
        self._validate_url()
        
        start_time = time.time()
        raw_data = []

        try:
            self.driver = self._setup_driver()
            
            # Rate limiting: respetar los servidores
            time.sleep(2)
            
            self.driver.get(self.url)

            # Esperar a que la tabla esté presente
            logger.info("Esperando carga de tabla...")
            wait = WebDriverWait(self.driver, 15)
            table = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "wikitable"))
            )

            logger.info("Tabla encontrada, extrayendo datos...")

            # Extraer headers
            headers_elements = table.find_elements(By.TAG_NAME, "th")
            headers = [header.text.strip() for header in headers_elements[:7]]
            logger.info(f"Headers encontrados: {headers}")

            # Extraer filas
            rows = table.find_elements(By.TAG_NAME, "tr")[2:]  # Saltar header y la primera fila que es el resumen

            for idx, row in enumerate(rows[:self.max_records], 1):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 6:
                        row_data = {
                            'rank': idx,
                            'country': self._safe_extract(cells[0]),
                            'population': self._safe_extract(cells[1]),
                            'percentage': self._safe_extract(cells[2]),
                            'date': self._safe_extract(cells[3]),
                            'source': self._safe_extract(cells[4]),
                        }
                        raw_data.append(row_data)

                        if idx % 10 == 0:
                            logger.info(f"Extraídos {idx} registros...")

                except Exception as e:
                    logger.warning(f"Error en fila {idx}: {str(e)}")
                    continue

            self.raw_data = raw_data
            self.metadata['extraction_time'] = time.time() - start_time
            self.metadata['total_records'] = len(raw_data)

            logger.info(
                f"Extracción completada: {len(raw_data)} registros en "
                f"{self.metadata['extraction_time']:.2f}s"
            )
            return raw_data

        except TimeoutException:
            logger.error("Timeout esperando la carga de la página")
            raise
        except Exception as e:
            logger.error(f"Error durante extracción: {str(e)}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver cerrado")

    def _safe_extract(self, element) -> str:
        """
        Extrae texto de forma segura manejando excepciones.

        Args:
            element: Elemento web a extraer

        Returns:
            Texto del elemento o cadena vacía
        """
        try:
            return element.text.strip()
        except Exception:
            return ""

    # ========================================================================
    # TRANSFORM
    # ========================================================================
    def transform(self) -> pd.DataFrame:
        """
        Transforma los datos extraídos aplicando limpieza y enriquecimiento.

        Returns:
            DataFrame transformado
            
        Raises:
            ValueError: Si no hay datos para transformar
        """
        logger.info("Iniciando transformación de datos...")
        start_time = time.time()

        if not self.raw_data:
            raise ValueError("No hay datos para transformar. Ejecute extract() primero.")

        # Crear DataFrame
        df = pd.DataFrame(self.raw_data)
        initial_count = len(df)
        logger.info(f"DataFrame creado con {initial_count} registros")

        # Pipeline de transformación
        df = self._clean_data(df)
        df = self._convert_types(df)
        df = self._validate_data(df)
        df = self._enrich_data(df)
        df = self._calculate_metrics(df)

        self.transformed_data = df
        self.metadata['transformation_time'] = time.time() - start_time
        self.metadata['valid_records'] = len(df)
        self.metadata['invalid_records'] = initial_count - len(df)

        logger.info(
            f"Transformación completada: {len(df)} registros válidos en "
            f"{self.metadata['transformation_time']:.2f}s"
        )
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia los datos removiendo caracteres especiales y normalizando."""
        logger.info("Limpiando datos...")

        # Limpiar población: remover comas, corchetes, notas
        df['population_clean'] = df['population'].apply(self._clean_population)

        # Limpiar porcentaje
        df['percentage_clean'] = df['percentage'].str.replace('%', '').str.strip()

        # Limpiar país (remover notas al pie)
        df['country_clean'] = df['country'].apply(
            lambda x: re.split(r'\[|\(', x)[0].strip()
        )

        return df

    def _clean_population(self, pop_str: str) -> str:
        """Limpia el string de población."""
        if not isinstance(pop_str, str):
            return ""
        # Remover todo excepto dígitos
        cleaned = re.sub(r'[^\d,]', '', pop_str)
        cleaned = cleaned.replace(',', '')
        return cleaned

    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte columnas a tipos apropiados."""
        logger.info("Convirtiendo tipos de datos...")

        df['population_numeric'] = pd.to_numeric(
            df['population_clean'], errors='coerce'
        )
        df['percentage_numeric'] = pd.to_numeric(
            df['percentage_clean'], errors='coerce'
        )
        df['rank_numeric'] = pd.to_numeric(df['rank'], errors='coerce')

        return df

    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida y filtra datos inválidos."""
        logger.info("Validando datos...")

        # Filtrar registros sin población válida
        df_valid = df[df['population_numeric'].notna()].copy()
        removed = len(df) - len(df_valid)

        if removed > 0:
            logger.warning(f"Removidos {removed} registros con población inválida")

        # Filtrar población > 0
        df_valid = df_valid[df_valid['population_numeric'] > 0]

        return df_valid

    def _enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece los datos con información adicional."""
        logger.info("Enriqueciendo datos...")

        # Categorizar países por población
        df['population_category'] = pd.cut(
            df['population_numeric'],
            bins=[0, 10_000_000, 50_000_000, 100_000_000, 500_000_000, float('inf')],
            labels=['Pequeño', 'Mediano', 'Grande', 'Muy Grande', 'Mega Poblado']
        )

        # Añadir timestamp de procesamiento
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['extraction_year'] = datetime.now().year

        return df

    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula métricas adicionales."""
        logger.info("Calculando métricas...")

        # Población en millones
        df['population_millions'] = (df['population_numeric'] / 1_000_000).round(2)

        # Porcentaje acumulado
        df['cumulative_percentage'] = df['percentage_numeric'].cumsum()

        # Ranking por densidad relativa
        df['relative_rank'] = df['population_numeric'].rank(
            ascending=False, method='dense'
        )

        # Diferencia con el país anterior
        df['population_diff'] = df['population_numeric'].diff().abs()
        df['population_diff_millions'] = (df['population_diff'] / 1_000_000).round(2)

        return df

    # ========================================================================
    # LOAD & ANALYSIS
    # ========================================================================
    def load(
        self, 
        output_format: str = 'csv', 
        filename: str = 'countries_data',
        output_dir: str = 'output'
    ) -> Path:
        """
        Carga los datos transformados en el formato especificado.

        Args:
            output_format: Formato de salida ('csv', 'excel', 'json', 'parquet')
            filename: Nombre del archivo de salida (sin extensión)
            output_dir: Directorio de salida
            
        Returns:
            Path del archivo guardado
            
        Raises:
            ValueError: Si no hay datos transformados o formato no soportado
        """
        if self.transformed_data is None:
            raise ValueError("No hay datos transformados. Ejecute transform() primero.")

        # Crear directorio de salida
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        logger.info(f"Cargando datos en formato {output_format}...")

        if output_format == 'csv':
            filepath = output_path / f"{filename}.csv"
            self.transformed_data.to_csv(
                filepath, index=False, encoding='utf-8-sig'
            )
        elif output_format == 'excel':
            filepath = output_path / f"{filename}.xlsx"
            self.transformed_data.to_excel(
                filepath, index=False, engine='openpyxl'
            )
        elif output_format == 'json':
            filepath = output_path / f"{filename}.json"
            self.transformed_data.to_json(
                filepath, orient='records', indent=2, force_ascii=False
            )
        elif output_format == 'parquet':
            filepath = output_path / f"{filename}.parquet"
            self.transformed_data.to_parquet(filepath, index=False)
        else:
            raise ValueError(
                f"Formato no soportado: {output_format}. "
                "Formatos válidos: csv, excel, json, parquet"
            )

        logger.info(f"Datos guardados exitosamente en: {filepath}")
        return filepath

    def get_summary_statistics(self) -> pd.DataFrame:
        """
        Retorna estadísticas descriptivas del dataset.
        
        Returns:
            DataFrame con estadísticas descriptivas
        """
        if self.transformed_data is None:
            raise ValueError("No hay datos transformados disponibles.")

        return self.transformed_data[[
            'population_numeric', 
            'percentage_numeric', 
            'population_millions'
        ]].describe()

    def get_metadata(self) -> Dict:
        """
        Retorna metadata del proceso ETL.
        
        Returns:
            Diccionario con metadata
        """
        return self.metadata

    def display_top_countries(self, n: int = 10) -> pd.DataFrame:
        """
        Muestra los top N países con formato mejorado.
        
        Args:
            n: Número de países a mostrar
            
        Returns:
            DataFrame con top N países
        """
        if self.transformed_data is None:
            raise ValueError("No hay datos transformados disponibles.")

        top_countries = self.transformed_data.head(n)[[
            'rank_numeric', 'country_clean', 'population_millions',
            'percentage_numeric', 'population_category'
        ]].copy()

        top_countries.columns = [
            'Ranking', 'País', 'Población (M)', '% Mundial', 'Categoría'
        ]

        return top_countries

    def run_full_pipeline(
        self, 
        output_formats: List[str] = None,
        output_filename: str = 'countries_population'
    ) -> Dict:
        """
        Ejecuta el pipeline ETL completo.
        
        Args:
            output_formats: Lista de formatos de salida
            output_filename: Nombre base para archivos de salida
            
        Returns:
            Diccionario con resultados del pipeline
        """
        if output_formats is None:
            output_formats = ['csv', 'excel']
        
        results = {
            'success': False,
            'files_created': [],
            'metadata': None,
            'error': None
        }
        
        try:
            # Extract
            logger.info("=" * 80)
            logger.info("FASE 1: EXTRACCIÓN")
            logger.info("=" * 80)
            self.extract()
            
            # Transform
            logger.info("\n" + "=" * 80)
            logger.info("FASE 2: TRANSFORMACIÓN")
            logger.info("=" * 80)
            self.transform()
            
            # Load
            logger.info("\n" + "=" * 80)
            logger.info("FASE 3: CARGA")
            logger.info("=" * 80)
            
            for fmt in output_formats:
                filepath = self.load(fmt, output_filename)
                results['files_created'].append(str(filepath))
            
            results['success'] = True
            results['metadata'] = self.get_metadata()
            
            logger.info("\n" + "=" * 80)
            logger.info("ETL COMPLETADO EXITOSAMENTE")
            logger.info("=" * 80)
            
            
            
        except Exception as e:
            logger.error(f"Error en pipeline ETL: {str(e)}")
            results['error'] = str(e)
            raise
        
        return results


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================
def main():
    """
    Función principal para ejecutar el pipeline ETL.
    
    Returns:
        Instancia del pipeline ETL ejecutado
    """
    print("=" * 80)
    print("ETL PIPELINE - PAÍSES MÁS POBLADOS DEL MUNDO")
    print("=" * 80)
    print()

    # Configuración
    url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
    
    # Crear instancia del ETL
    etl = WikipediaCountriesETL(url, max_records=50)

    try:
        # Ejecutar pipeline completo
        results = etl.run_full_pipeline(
            output_formats=['csv', 'excel', 'json'],
            output_filename='countries_population'
        )
        
        # Mostrar resultados
        print("\n" + "=" * 80)
        print("RESULTADOS DEL ETL")
        print("=" * 80)
        
        metadata = etl.get_metadata()
        print(f"\nTiempo de extracción: {metadata['extraction_time']:.2f}s")
        print(f"Tiempo de transformación: {metadata['transformation_time']:.2f}s")
        print(f"Total de registros: {metadata['total_records']}")
        print(f"Registros válidos: {metadata['valid_records']}")
        print(f"Registros inválidos: {metadata['invalid_records']}")
        
        print("\nArchivos generados:")
        for file in results['files_created']:
            print(f"   - {file}")
        
        # Top 10 países
        print("\n" + "=" * 80)
        print("TOP 10 PAÍSES MÁS POBLADOS")
        print("=" * 80)
        top_10 = etl.display_top_countries(10)
        print(top_10.to_string(index=False))

        # Estadísticas descriptivas
        print("\n" + "=" * 80)
        print("ESTADÍSTICAS DESCRIPTIVAS")
        print("=" * 80)
        stats = etl.get_summary_statistics()
        print(stats)

        # Análisis adicional
        print("\n" + "=" * 80)
        print("ANÁLISIS ADICIONAL")
        print("=" * 80)
        df = etl.transformed_data

        print(f"• Población total (top {etl.max_records}): {df['population_numeric'].sum():,.0f} habitantes")
        print(f"• Población promedio: {df['population_numeric'].mean():,.0f} habitantes")
        print(f"• País más poblado: {df.iloc[0]['country_clean']} ({df.iloc[0]['population_millions']:.2f}M)")
        
        print("\n• Distribución por categoría:")
        print(df['population_category'].value_counts().to_string())

        # Porcentaje acumulado
        top_10_percentage = df.head(10)['percentage_numeric'].sum()
        print(f"\n• Top 10 países representan: {top_10_percentage:.2f}% de la población mundial")

        #-------------------------------------------
        # VISUALIZACION DASHBOARD
        #-------------------------------------------

        logger.info("\n" + "=" * 80)
        logger.info("DASHBOARD INTERACTIVO TOP 50 PAÍSES")
        logger.info("=" * 80)
            

        try:
                dashboard = dashboard_module.create_dashboard_from_file(
                    'output/countries_population.csv',
                    title="World Population Analytics Dashboard"
                )
                dashboard.create_app()
                dashboard.run(debug=True, port=8050)
            
        except FileNotFoundError:
                print("Archivo no encontrado. Ejecute primero el ETL pipeline.")
                print("python etl_wikipedia_countries.py")
        #-------------------------------------------


        return etl

    except Exception as e:
        logger.error(f"Error en el pipeline ETL: {str(e)}")
        raise


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================
if __name__ == "__main__":
    etl_pipeline = main()
    
    
