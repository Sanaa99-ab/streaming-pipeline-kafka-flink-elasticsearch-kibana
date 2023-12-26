#from pyflink.table.descriptors import Elasticsearch, FileSystem, Schema
from pyflink.table import DataTypes
from pyflink.table import EnvironmentSettings, TableEnvironment

# Create a table environment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///home/sanaa/Documents/v2/venv2_kafka_flink/flink-sql-connector-kafka-3.0.2-1.17.jar;"
    "file:///home/sanaa/Documents/v3/venv3_flink_elasticsearch/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"
    
)  


# Define source table DDL for Kafka
source_ddl = """
    CREATE TABLE source_table (
        id STRING,
        symbol STRING,
        name STRING,
        image STRING,
        current_price DOUBLE,
        market_cap BIGINT,
        market_cap_rank INT,
        fully_diluted_valuation BIGINT,
        total_volume BIGINT,
        high_24h DOUBLE,
        low_24h DOUBLE,
        price_change_24h DOUBLE,
        price_change_percentage_24h DOUBLE,
        market_cap_change_24h BIGINT,
        market_cap_change_percentage_24h DOUBLE,
        circulating_supply DOUBLE,
        total_supply DOUBLE,
        max_supply DOUBLE,
        ath DOUBLE,
        ath_change_percentage DOUBLE,
        ath_date STRING,
        atl DOUBLE,
        atl_change_percentage DOUBLE,
        atl_date STRING,
        roi ROW<times DOUBLE, currency STRING, percentage DOUBLE>
        
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'cryptocurrency_data',
        'properties.bootstrap.servers' = 'localhost:9092',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""

sink_ddl = """
    CREATE TABLE sink_table (
       id STRING,
        symbol STRING,
        name STRING,
        image STRING,
        current_price DOUBLE,
        market_cap BIGINT,
        market_cap_rank INT,
        fully_diluted_valuation BIGINT,
        total_volume BIGINT,
        high_24h DOUBLE,
        low_24h DOUBLE,
        price_change_24h DOUBLE,
        price_change_percentage_24h DOUBLE,
        market_cap_change_24h BIGINT,
        market_cap_change_percentage_24h DOUBLE,
        circulating_supply DOUBLE,
        total_supply DOUBLE,
        max_supply DOUBLE,
        ath DOUBLE,
        ath_change_percentage DOUBLE,
        ath_date STRING,
        atl DOUBLE,
        atl_change_percentage DOUBLE,
        atl_date STRING,
        roi ROW<times DOUBLE, currency STRING, percentage DOUBLE>
    ) WITH (
        'connector' = 'elasticsearch-7',
        'hosts' = 'http://localhost:9200',
        'index' = 'word_count',
        'format' = 'json'
       
    )
"""

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

# Retrieve the source table


# Process the data
# Fetch the data from the source table
source_table = t_env.from_path('source_table')


# Print the data
'''for row in result_table:
    print(row)
'''
# Retrieve the sink table
sink_table = t_env.from_path('sink_table')

print("Sink Table Schema:")
sink_table.print_schema()

# Insert the processed data into the sink table



# Retrieve the sink table


# Insert the processed data into the sink table
source_table.execute_insert('sink_table').wait()