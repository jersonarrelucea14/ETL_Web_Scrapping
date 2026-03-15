"""
Dashboard Interactivo para Visualización de Datos de Población Mundial.

Este módulo implementa un dashboard con Plotly Dash que incluye:
- Visualizaciones interactivas de población mundial
- Análisis comparativo entre países
- Métricas clave y KPIs
- Filtros dinámicos y controles
- Diseño profesional y responsive

Autor: Jerson Arrelucea
Fecha: 2026-01-11

INSTALACIÓN:
    pip install dash dash-bootstrap-components plotly pandas


"""

# ============================================================================
# IMPORTS
# ============================================================================
import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from pathlib import Path
import logging

# ============================================================================
# CONFIGURACIÓN
# ============================================================================
logger = logging.getLogger(__name__)

# Paleta de colores profesional
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'danger': '#d62728',
    'warning': '#ff9800',
    'info': '#17a2b8',
    'dark': '#2c3e50',
    'light': '#ecf0f1',
    'background': '#f8f9fa',
    'card': '#ffffff',
    'gradient_start': '#667eea',
    'gradient_end': '#764ba2'
}


# ============================================================================
# CLASE DASHBOARD
# ============================================================================
class CountriesPopulationDashboard:
    """
    Dashboard interactivo para análisis de población mundial.
    
    Attributes:
        df (pd.DataFrame): Datos de países
        app (dash.Dash): Aplicación Dash
    """
    
    def __init__(self, dataframe: pd.DataFrame, title: str = "World Population Dashboard"):
        """
        Inicializa el dashboard.
        
        Args:
            dataframe: DataFrame con datos de países
            title: Título del dashboard
        """
        self.df = dataframe.copy()
        self.title = title
        self.app = None
        self._prepare_data()
        
    def _prepare_data(self):
        """Prepara y enriquece los datos para visualización."""
        # Asegurar que tenemos todas las columnas necesarias
        required_cols = ['country_clean', 'population_numeric', 'percentage_numeric']
        for col in required_cols:
            if col not in self.df.columns:
                raise ValueError(f"Columna requerida no encontrada: {col}")
        
        # Ordenar por población
        self.df = self.df.sort_values('population_numeric', ascending=False).reset_index(drop=True)
        
        # Agregar continente si no existe (simplificado)
        if 'continent' not in self.df.columns:
            self.df['continent'] = self._assign_continents()
    
    def _assign_continents(self) -> List[str]:
        """Asigna continentes a países (simplificado)."""
        continent_map = {
            'China': 'Asia', 'India': 'Asia', 'United States': 'Americas',
            'Indonesia': 'Asia', 'Pakistan': 'Asia', 'Nigeria': 'Africa',
            'Brazil': 'Americas', 'Bangladesh': 'Asia', 'Russia': 'Europe',
            'Mexico': 'Americas', 'Japan': 'Asia', 'Ethiopia': 'Africa',
            'Philippines': 'Asia', 'Egypt': 'Africa', 'Vietnam': 'Asia',
            'DR Congo': 'Africa', 'Turkey': 'Asia', 'Iran': 'Asia',
            'Germany': 'Europe', 'Thailand': 'Asia', 'United Kingdom': 'Europe',
            'France': 'Europe', 'Tanzania': 'Africa', 'South Africa': 'Africa',
            'Italy': 'Europe', 'Kenya': 'Africa', 'Myanmar': 'Asia',
            'Colombia': 'Americas', 'South Korea': 'Asia', 'Spain': 'Europe',
            'Argentina': 'Americas', 'Algeria': 'Africa', 'Sudan': 'Africa',
            'Uganda': 'Africa', 'Ukraine': 'Europe', 'Iraq': 'Asia',
            'Afghanistan': 'Asia', 'Poland': 'Europe', 'Canada': 'Americas',
            'Morocco': 'Africa', 'Saudi Arabia': 'Asia', 'Uzbekistan': 'Asia',
            'Peru': 'Americas', 'Angola': 'Africa', 'Malaysia': 'Asia',
            'Mozambique': 'Africa', 'Ghana': 'Africa', 'Yemen': 'Asia',
            'Nepal': 'Asia', 'Venezuela': 'Americas', 'Madagascar': 'Africa',
            'Australia': 'Oceania'
        }
        
        return [continent_map.get(country, 'Other') for country in self.df['country_clean']]
    
    def create_app(self) -> dash.Dash:
        """
        Crea la aplicación Dash con todos los componentes.
        
        Returns:
            Aplicación Dash configurada
        """
        # Inicializar app con tema Bootstrap
        self.app = dash.Dash(
            __name__,
            external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
            meta_tags=[
                {"name": "viewport", "content": "width=device-width, initial-scale=1"}
            ]
        )
        
        self.app.title = self.title
        
        # Layout del dashboard
        self.app.layout = self._create_layout()
        
        # Registrar callbacks
        self._register_callbacks()
        
        return self.app
    
    def _create_layout(self) -> html.Div:
        """Crea el layout completo del dashboard."""
        return html.Div([
            # Header
            self._create_header(),
            
            # KPIs
            dbc.Container([
                self._create_kpi_cards(),
            ], fluid=True, className="mb-4"),
            
            # Main Content
            dbc.Container([
                # Controles
                self._create_controls(),
                
                # Gráficos principales
                dbc.Row([
                    dbc.Col([
                        self._create_card(
                            "Top Países por Población",
                            dcc.Graph(id='bar-chart', config={'displayModeBar': False})
                        )
                    ], md=6),
                    dbc.Col([
                        self._create_card(
                            "Distribución Global",
                            dcc.Graph(id='pie-chart', config={'displayModeBar': False})
                        )
                    ], md=6),
                ], className="mb-4"),
                
                # Gráficos secundarios
                dbc.Row([
                    dbc.Col([
                        self._create_card(
                            "Evolución de Población Acumulada",
                            dcc.Graph(id='cumulative-chart', config={'displayModeBar': False})
                        )
                    ], md=8),
                    dbc.Col([
                        self._create_card(
                            "Categorización de Países",
                            dcc.Graph(id='category-chart', config={'displayModeBar': False})
                        )
                    ], md=4),
                ], className="mb-4"),
                
                # Treemap y Tabla
                dbc.Row([
                    dbc.Col([
                        self._create_card(
                            "Mapa Jerárquico por Continente",
                            dcc.Graph(id='treemap', config={'displayModeBar': False})
                        )
                    ], md=7),
                    dbc.Col([
                        self._create_card(
                            "Datos Detallados",
                            html.Div(id='data-table')
                        )
                    ], md=5),
                ], className="mb-4"),
                
            ], fluid=True),
            
            # Footer
            self._create_footer(),
            
        ], style={'backgroundColor': COLORS['background'], 'minHeight': '100vh'})
    
    def _create_header(self) -> dbc.Navbar:
        """Crea el header del dashboard."""
        return dbc.Navbar(
            dbc.Container([
                dbc.Row([
                    dbc.Col([
                        html.I(className="fas fa-globe fa-2x text-white me-3"),
                        dbc.NavbarBrand(
                            self.title,
                            className="fs-3 fw-bold text-white"
                        ),
                    ], className="d-flex align-items-center"),
                ], className="w-100"),
            ], fluid=True),
            color="dark",
            dark=True,
            className="mb-4 shadow",
            style={
                'background': f'linear-gradient(135deg, {COLORS["gradient_start"]} 0%, {COLORS["gradient_end"]} 100%)'
            }
        )
    
    def _create_kpi_cards(self) -> dbc.Row:
        """Crea las tarjetas de KPIs."""
        total_pop = self.df['population_numeric'].sum()
        avg_pop = self.df['population_numeric'].mean()
        total_countries = len(self.df)
        top_10_pct = self.df.head(10)['percentage_numeric'].sum()
        
        kpis = [
            {
                'icon': 'fa-users',
                'title': 'Población Total',
                'value': f"{total_pop/1e9:.2f}B",
                'subtitle': f'En {total_countries} países',
                'color': 'primary'
            },
            {
                'icon': 'fa-chart-line',
                'title': 'Promedio por País',
                'value': f"{avg_pop/1e6:.1f}M",
                'subtitle': 'Habitantes',
                'color': 'success'
            },
            {
                'icon': 'fa-trophy',
                'title': 'Top 10 Representa',
                'value': f"{top_10_pct:.1f}%",
                'subtitle': 'De la población mundial',
                'color': 'warning'
            },
            {
                'icon': 'fa-flag',
                'title': 'Total de Países',
                'value': str(total_countries),
                'subtitle': 'Analizados',
                'color': 'info'
            },
        ]
        
        return dbc.Row([
            dbc.Col([
                self._create_kpi_card(**kpi)
            ], md=3) for kpi in kpis
        ], className="mb-4")
    
    def _create_kpi_card(self, icon: str, title: str, value: str, 
                         subtitle: str, color: str) -> dbc.Card:
        """Crea una tarjeta KPI individual."""
        return dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.I(className=f"fas {icon} fa-3x text-{color}")
                    ], width=4, className="d-flex align-items-center justify-content-center"),
                    dbc.Col([
                        html.H6(title, className="text-muted mb-1"),
                        html.H3(value, className=f"mb-0 fw-bold text-{color}"),
                        html.Small(subtitle, className="text-muted"),
                    ], width=8),
                ]),
            ])
        ], className="shadow-sm border-0 h-100")
    
    def _create_controls(self) -> dbc.Card:
        """Crea los controles del dashboard."""
        return dbc.Card([
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.Label("Número de Países a Mostrar:", className="fw-bold"),
                        dcc.Slider(
                            id='top-n-slider',
                            min=5,
                            max=50,
                            step=5,
                            value=20,
                            marks={i: str(i) for i in range(5, 51, 5)},
                            tooltip={"placement": "bottom", "always_visible": True}
                        ),
                    ], md=6),
                    dbc.Col([
                        html.Label("Filtrar por Continente:", className="fw-bold"),
                        dcc.Dropdown(
                            id='continent-filter',
                            options=[
                                {'label': 'Todos', 'value': 'all'},
                                {'label': 'Asia', 'value': 'Asia'},
                                {'label': 'África', 'value': 'Africa'},
                                {'label': 'Europa', 'value': 'Europe'},
                                {'label': 'Américas', 'value': 'Americas'},
                                {'label': 'Oceanía', 'value': 'Oceania'},
                            ],
                            value='all',
                            clearable=False,
                        ),
                    ], md=3),
                    dbc.Col([
                        html.Label("Tipo de Visualización:", className="fw-bold"),
                        dcc.Dropdown(
                            id='chart-type',
                            options=[
                                {'label': '📊 Barras', 'value': 'bar'},
                                {'label': '📈 Líneas', 'value': 'line'},
                            ],
                            value='bar',
                            clearable=False,
                        ),
                    ], md=3),
                ], className="align-items-end"),
            ])
        ], className="mb-4 shadow-sm border-0")
    
    def _create_card(self, title: str, content) -> dbc.Card:
        """Crea una tarjeta genérica."""
        return dbc.Card([
            dbc.CardHeader([
                html.H5(title, className="mb-0 fw-bold text-dark")
            ], className="bg-light"),
            dbc.CardBody(content, className="p-3"),
        ], className="shadow-sm border-0 h-100")
    
    def _create_footer(self) -> html.Footer:
        """Crea el footer del dashboard."""
        return html.Footer([
            dbc.Container([
                html.Hr(),
                dbc.Row([
                    dbc.Col([
                        html.P([
                            "📊 Dashboard de Población Mundial | ",
                            html.Strong("ETL Pipeline Project"),
                            " | Datos de Wikipedia"
                        ], className="text-muted text-center mb-0"),
                    ]),
                ]),
            ], fluid=True),
        ], className="mt-5 py-3 bg-white")
    
    def _register_callbacks(self):
        """Registra todos los callbacks del dashboard."""
        
        @self.app.callback(
            [
                Output('bar-chart', 'figure'),
                Output('pie-chart', 'figure'),
                Output('cumulative-chart', 'figure'),
                Output('category-chart', 'figure'),
                Output('treemap', 'figure'),
                Output('data-table', 'children'),
            ],
            [
                Input('top-n-slider', 'value'),
                Input('continent-filter', 'value'),
                Input('chart-type', 'value'),
            ]
        )
        def update_charts(top_n, continent, chart_type):
            """Actualiza todos los gráficos según los filtros."""
            # Filtrar datos
            df_filtered = self.df.copy()
            if continent != 'all':
                df_filtered = df_filtered[df_filtered['continent'] == continent]
            
            df_top = df_filtered.head(top_n)
            
            # 1. Gráfico de barras/líneas
            if chart_type == 'bar':
                fig_bar = px.bar(
                    df_top,
                    x='country_clean',
                    y='population_millions',
                    color='population_millions',
                    color_continuous_scale='Viridis',
                    labels={
                        'country_clean': 'País',
                        'population_millions': 'Población (Millones)'
                    },
                    title=f'Top {top_n} Países'
                )
                fig_bar.update_layout(
                    xaxis_tickangle=-45,
                    showlegend=False,
                    hovermode='x unified'
                )
            else:
                fig_bar = px.line(
                    df_top,
                    x='country_clean',
                    y='population_millions',
                    markers=True,
                    labels={
                        'country_clean': 'País',
                        'population_millions': 'Población (Millones)'
                    },
                    title=f'Top {top_n} Países'
                )
                fig_bar.update_traces(
                    line=dict(color=COLORS['primary'], width=3),
                    marker=dict(size=8)
                )
                fig_bar.update_layout(xaxis_tickangle=-45, hovermode='x unified')
            
            # 2. Gráfico de pastel
            fig_pie = px.pie(
                df_top,
                values='population_numeric',
                names='country_clean',
                title='Distribución Porcentual',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            
            # 3. Gráfico acumulado
            df_cumsum = df_top.copy()
            df_cumsum['cumulative_pop'] = df_cumsum['population_numeric'].cumsum() / 1e9
            
            fig_cumulative = go.Figure()
            fig_cumulative.add_trace(go.Scatter(
                x=df_cumsum['country_clean'],
                y=df_cumsum['cumulative_pop'],
                mode='lines+markers',
                fill='tozeroy',
                name='Acumulado',
                line=dict(color=COLORS['success'], width=3),
                marker=dict(size=8)
            ))
            fig_cumulative.update_layout(
                xaxis_title='País',
                yaxis_title='Población Acumulada (Miles de Millones)',
                xaxis_tickangle=-45,
                hovermode='x unified',
                title=f'Población Acumulada - Top {top_n}'
            )
            
            # 4. Gráfico de categorías
            if 'population_category' in df_top.columns:
                category_counts = df_top['population_category'].value_counts()
                fig_category = px.bar(
                    x=category_counts.index,
                    y=category_counts.values,
                    labels={'x': 'Categoría', 'y': 'Cantidad'},
                    title='Países por Categoría',
                    color=category_counts.values,
                    color_continuous_scale='Blues'
                )
                fig_category.update_layout(showlegend=False)
            else:
                fig_category = go.Figure()
            
            # 5. Treemap
            fig_treemap = px.treemap(
                df_top,
                path=['continent', 'country_clean'],
                values='population_numeric',
                color='population_millions',
                color_continuous_scale='RdYlGn',
                title='Mapa Jerárquico por Continente'
            )
            
            # 6. Tabla de datos
            table = dbc.Table.from_dataframe(
                df_top[['country_clean', 'population_millions', 'percentage_numeric']]
                .rename(columns={
                    'country_clean': 'País',
                    'population_millions': 'Población (M)',
                    'percentage_numeric': '% Mundial'
                })
                .head(15),
                striped=True,
                bordered=True,
                hover=True,
                responsive=True,
                className="table-sm"
            )
            
            # Aplicar tema común
            for fig in [fig_bar, fig_pie, fig_cumulative, fig_category, fig_treemap]:
                fig.update_layout(
                    template='plotly_white',
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(t=40, l=0, r=0, b=0),
                    font=dict(family="Arial, sans-serif", size=12),
                    title_font_size=16,
                    title_font_color=COLORS['dark']
                )
            
            return fig_bar, fig_pie, fig_cumulative, fig_category, fig_treemap, table
    
    def run(self, debug: bool = True, port: int = 8050, host: str = '0.0.0.0'):
        """
        Ejecuta el servidor del dashboard.
        
        Args:
            debug: Modo debug
            port: Puerto del servidor
            host: Host del servidor
        """
        if self.app is None:
            self.create_app()
        
        print("=" * 80)
        print(f"Dashboard iniciado en http://localhost:{port}")
        print("=" * 80)
        print("Presiona Ctrl+C para detener el servidor")
        print()
        
        # Usar app.run() para versiones modernas de Dash
        self.app.run(debug=debug, port=port, host=host)


# ============================================================================
# FUNCIÓN DE UTILIDAD
# ============================================================================
def create_dashboard_from_file(
    filepath: str,
    title: str = "World Population Dashboard"
) -> CountriesPopulationDashboard:
    """
    Crea un dashboard desde un archivo CSV/Excel/Parquet.
    
    Args:
        filepath: Ruta al archivo de datos
        title: Título del dashboard
        
    Returns:
        Instancia del dashboard
    """
    file_path = Path(filepath)
    
    if not file_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
    
    # Cargar datos según extensión
    if file_path.suffix == '.csv':
        df = pd.read_csv(filepath)
    elif file_path.suffix in ['.xlsx', '.xls']:
        df = pd.read_excel(filepath)
    elif file_path.suffix == '.parquet':
        df = pd.read_parquet(filepath)
    else:
        raise ValueError(f"Formato no soportado: {file_path.suffix}")
    
    return CountriesPopulationDashboard(df, title)


# ============================================================================
# EJEMPLO DE USO
# ============================================================================
if __name__ == "__main__":
    # Opción 1: Desde un archivo
    try:
        dashboard = create_dashboard_from_file(
            'output/countries_population.csv',
            title="World Population Analytics Dashboard"
        )
        dashboard.create_app()
        dashboard.run(debug=True, port=8050)
    
    except FileNotFoundError:
        print("Archivo no encontrado. Ejecute primero el ETL pipeline.")
        print("python etl_wikipedia_countries.py")
    
    # Opción 2: Desde un DataFrame existente (si ya tienes datos)
    # dashboard = CountriesPopulationDashboard(your_dataframe)
    # dashboard.create_app()
    # dashboard.run()
    
    
