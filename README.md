E-commerce Data Warehouse
Este proyecto implementa un data warehouse completo para un e-commerce siguiendo la arquitectura de medallones (Bronze/Silver/Gold) en Snowflake mediante dbt.


Estructura del Proyecto


curso_data_engineering/
├── analyses/          # Análisis ad-hoc y queries exploratorios
├── macros/           # Funciones reutilizables para transformaciones
├── models/           # Modelos principales organizados por capas
│   ├── staging/      # Capa Bronze - datos limpios de fuentes
│   │   ├── google_sheets/
│   │   └── sql_server_dbo/
│   ├── silver/       # Capa Silver - dimensiones y hechos del negocio
│   │   ├── dimensions/
│   │   └── facts/
│   └── gold/         # Capa Gold - reportes y analytics
├── seeds/            # Datos estáticos para enriquecer el DW
├── snapshots/        # Capturas de datos que cambian lentamente
└── tests/            # Pruebas para validar la calidad de datos


Capas del Data Warehouse

Bronze (Staging)
Transforma datos crudos desde las fuentes en un formato consistente y limpio. Implementa tipado estricto y filtrado básico.
Silver (Integration)
Organizada en:

Dimensions: Entidades de negocio como usuarios, productos y direcciones
Facts: Eventos transaccionales como órdenes, ítems de orden y eventos de usuario

Gold (Analytics)
Reportes optimizados para consumo por BI y equipos de negocio:

rpt_sales_performance: Análisis consolidado de ventas
rpt_user_behavior: Comportamiento de usuario y segmentación

Instalación
bash# Clonar el repositorio
git clone https://github.com/tu-usuario/curso_data_engineering.git

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
