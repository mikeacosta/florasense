import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get('IAM_ROLE','ARN')
READING_DATA = config.get('S3','READING_DATA')
CUSTOMER_DATA = config.get('S3','CUSTOMER_DATA')
SENSOR_DATA = config.get('S3','SENSOR_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_customers_table_drop = "DROP TABLE IF EXISTS staging_customers"
staging_sensors_table_drop = "DROP TABLE IF EXISTS staging_sensors"
sensorreadings_table_drop = "DROP TABLE IF EXISTS sensorreadings;"
sensors_table_drop = "DROP TABLE IF EXISTS sensors;"
customers_table_drop = "DROP TABLE IF EXISTS customers;"
time_table_drop = "DROP TABLE IF EXISTS time;"
locations_table_drop = "DROP TABLE IF EXISTS locations;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        ambient_temperature float,
        humidity            float,
        photosensor         float,
        timestamp           integer,
        sensor_number       integer,
        reading_id          varchar
    );
""")

staging_customers_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_customers(
        customer_id     varchar,
        company         varchar,
        city            varchar,
        state           varchar     
    );
""")

staging_sensors_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_sensors(
        sensor_number           integer,
        sensor_name             varchar,
        description             varchar,
        sku                     varchar,
        customer_id             varchar,
        company                 varchar,
        city                    varchar,
        state                   varchar,
        location_id             integer,
        location_description    varchar,
        latitude                float,
        longitude               float
    );
""")

sensorreadings_table_create = ("""
    CREATE TABLE IF NOT EXISTS sensorreadings 
    (
        reading_id          varchar NOT NULL PRIMARY KEY,
        sensor_number       integer NOT NULL,
        timestamp           timestamp NOT NULL,
        customer_id         varchar NOT NULL,
        location_id         integer,
        ambient_temperature float,
        humidity            float,
        light_level         float
    );
""")

sensors_table_create = ("""
    CREATE TABLE IF NOT EXISTS sensors
    (
        sensor_number   integer NOT NULL PRIMARY KEY,
        sensor_name     varchar,
        description     varchar,
        customer_id     varchar,
        sku             varchar
    );
""")

customers_table_create = ("""
    CREATE TABLE IF NOT EXISTS customers
    (
        customer_id     varchar NOT NULL PRIMARY KEY,
        company         varchar,
        city            varchar,
        state           varchar
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        timestamp   timestamp NOT NULL PRIMARY KEY SORTKEY DISTKEY,
        month       integer NOT NULL,
        date        integer NOT NULL,
        year        integer NOT NULL,
        hour        integer NOT NULL,
        minute      integer NOT NULL,
        second      integer NOT NULL,
        weekday     integer NOT NULL
    );
""")

locations_table_create = ("""
    CREATE TABLE IF NOT EXISTS locations
    (
        location_id     integer NOT NULL PRIMARY KEY,
        description     varchar,
        latitude        float,
        longitude       float   
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto'
    timeformat as 'epochmillisecs'
    TRUNCATECOLUMNS;
""").format(READING_DATA, IAM_ROLE_ARN)

staging_customers_copy = ("""
    copy staging_customers from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto'
    TRUNCATECOLUMNS;
""").format(CUSTOMER_DATA, IAM_ROLE_ARN)

staging_sensors_copy = ("""
    copy staging_sensors from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    CSV
    IGNOREHEADER 1;
""").format(SENSOR_DATA, IAM_ROLE_ARN)

# FINAL TABLES

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_customers_table_create, staging_sensors_table_create, sensorreadings_table_create, sensors_table_create, customers_table_create, time_table_create, locations_table_create]
drop_table_queries = [staging_events_table_drop, staging_customers_table_drop, staging_sensors_table_drop, sensorreadings_table_drop, sensors_table_drop, customers_table_drop, time_table_drop, locations_table_drop]
copy_table_queries = [staging_events_copy, staging_customers_copy, staging_sensors_copy]
insert_table_queries = []