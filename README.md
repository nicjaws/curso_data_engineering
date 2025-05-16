# E-commerce Data Warehouse
Este proyecto implementa un data warehouse completo para un e-commerce siguiendo la arquitectura de medallones (Bronze/Silver/Gold) en Snowflake mediante dbt.


# Estructura del Proyecto

<img width="528" alt="Captura de pantalla 2025-05-16 a las 13 35 24" src="https://github.com/user-attachments/assets/946f322e-7b2b-47ad-8007-364253b6d92c" />



# Capas del Data Warehouse

Bronze (Staging)
Transforma datos crudos desde las fuentes en un formato consistente y limpio. Implementa tipado estricto y filtrado básico.
Silver (Integration)


# Organizada en:

Dimensions: Entidades de negocio como usuarios, productos y direcciones
Facts: Eventos transaccionales como órdenes, ítems de orden y eventos de usuario

Gold (Analytics)
Reportes optimizados para consumo por BI y equipos de negocio:

rpt_sales_performance: Análisis consolidado de ventas
rpt_user_behavior: Comportamiento de usuario y segmentación

Instalación
bash# Clonar el repositorio
git clone https://github.com/nicjaws/curso_data_engineering.git

# Instalar dependencias
cd curso_data_engineering
dbt deps
Ejecución
bash# Ejecutar todo el proyecto
dbt build

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
