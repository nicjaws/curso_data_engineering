# Data Warehouse para E-commerce 📊

Una implementación completa de un data warehouse para análisis de comercio electrónico construido con DBT y Snowflake, siguiendo el patrón de arquitectura de medallones (Bronze/Silver/Gold).
Mostrar imagen
🏗️ Visión General de la Arquitectura
Este proyecto implementa un data warehouse moderno siguiendo la arquitectura de medallones:


🥉 Capa Bronze (Datos Crudos)

# Datos crudos de diversas fuentes

SQL Server (pedidos, productos, usuarios)
Google Sheets (datos de presupuesto)
Transformaciones mínimas, solo carga



-- 🥈 Capa Silver (Integración)
Ubicada en models/silver/:

Dimensiones: Entidades de negocio enriquecidas (usuarios, productos, direcciones, promociones)
Hechos: Datos transaccionales limpios (pedidos, ítems de pedido, eventos)
Verificaciones de calidad de datos e integración de múltiples fuentes
Implementación de lógica de negocio

-- 🥇 Capa Gold (Presentación)
Ubicada en models/marts/:

Dimensiones: Entidades amigables para el negocio listas para análisis
Hechos: Datos transaccionales optimizados con métricas calculadas
Reportes: Modelos analíticos agregados para usuarios de negocio

rpt_sales_performance: Análisis consolidado de ventas
rpt_user_behavior: Segmentación y análisis del comportamiento del cliente



# 📊 Reportes Analíticos Clave
Análisis de Rendimiento de Ventas

Tendencias de ventas diarias y mensuales
Métricas de rendimiento a nivel de producto
Análisis de distribución del estado de pedidos
Métricas de ingresos por cliente

Análisis de Comportamiento del Usuario

Segmentación RFM (Recencia, Frecuencia, Valor Monetario)
Cálculos del valor del cliente a lo largo del tiempo
Análisis de ciclo de conversión
Métricas de compromiso y actividad

# 📋 Modelos de Datos
Dimensiones

dim_users: Perfiles de usuarios con información demográfica
dim_products: Catálogo de productos con categorización y estado de inventario
dim_addresses: Ubicaciones geográficas con enriquecimiento regional
dim_promos: Promociones y descuentos disponibles

Hechos

fact_orders: Pedidos con métricas de tiempo y estado
fact_order_items: Líneas de pedido detalladas con información de producto
fact_events: Eventos de usuario para análisis de comportamiento

# 🛠️ Instalación y Configuración
Prerrequisitos

Cuenta de Snowflake
DBT Core instalado (v1.0.0+)
Python 3.8+

Pasos de Instalación
bash# Clonar el repositorio
git clone https://github.com/nicjaws/ecommerce-data-warehouse.git

# Navegar al directorio
cd ecommerce-data-warehouse

# Instalar dependencias
dbt deps
Configuración

Configura tu perfil de DBT en ~/.dbt/profiles.yml
Verifica la conexión con Snowflake:

bashdbt debug
🚀 Ejecución
Construir el Proyecto Completo
bashdbt build
Ejecutar Capas Específicas
bash# Solo capa staging
dbt build --select staging

# Solo capa silver
dbt build --select silver

# Solo capa gold
dbt build --select gold
Ejecutar Pruebas
bashdbt test
📐 Paquetes DBT Utilizados

dbt-labs/codegen (v0.13.1): Generación automática de código
dbt-labs/dbt_utils (v1.3.0): Utilidades y funciones comunes
metaplane/dbt_expectations (v0.10.8): Pruebas avanzadas para calidad de datos

🧩 Linaje y Estructura del Proyecto

<img width="1101" alt="Captura de pantalla 2025-05-20 a las 11 07 49" src="https://github.com/user-attachments/assets/dc899773-fe3f-4d20-8338-b4affe657537" />

![Captura de pantalla 2025-05-20 194544](https://github.com/user-attachments/assets/7f39472b-59af-4c71-b8db-bf19cda49d7a)

# 📈 Casos de Uso Recomendados

Análisis de Ventas: Utiliza rpt_sales_performance para analizar tendencias de ventas.
Segmentación de Clientes: Utiliza rpt_user_behavior para segmentar clientes y analizar su valor.
Optimización de Inventario: Combina dim_products con fact_orders para gestionar niveles de inventario.
Análisis de Embudos de Conversión: Analiza fact_events para optimizar embudos de conversión.

# 👥 Contribución
Si deseas contribuir a este proyecto:

Haz un fork del repositorio
Crea una rama para tu funcionalidad (git checkout -b feature/amazing-feature)
Confirma tus cambios (git commit -m 'feat: agregar nueva funcionalidad')
Empuja a la rama (git push origin feature/amazing-feature)
Abre un Pull Request



# Ejecutar solo modelos de una capa específica
dbt build --select staging
dbt build --select silver
dbt build --select gold

# Ejecutar tests
dbt test
Paquetes Instalados

dbt-labs/codegen (v0.13.1): Generación automática de código
dbt-labs/dbt_utils (v1.3.0): Utilidades y funciones comunes
metaplane/dbt_expectations (v0.10.8): Tests avanzados para calidad de datos
