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
        reading_id          varchar NOT NULL PRIMARY KEY DISTKEY,
        timestamp           timestamp NOT NULL,
        sensor_number       integer NOT NULL,
        customer_id         varchar NOT NULL,
        location_id         integer,
        ambient_temperature float,
        humidity            float,
        light_level         float
    )
    SORTKEY (timestamp, sensor_number);
""")

sensors_table_create = ("""
    CREATE TABLE IF NOT EXISTS sensors
    (
        sensor_number   integer NOT NULL PRIMARY KEY SORTKEY,
        sensor_name     varchar,
        description     varchar,
        customer_id     varchar DISTKEY,
        sku             varchar
    );
""")

customers_table_create = ("""
    CREATE TABLE IF NOT EXISTS customers
    (
        customer_id     varchar NOT NULL PRIMARY KEY SORTKEY,
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
        location_id     integer NOT NULL PRIMARY KEY SORTKEY,
        description     varchar,
        latitude        float,
        longitude       float   
    );
""")

# DELETE TABLES

staging_events_delete = ("""
    DELETE FROM staging_events
""")

staging_customers_delete = ("""
    DELETE FROM staging_customers
""")

staging_sensors_delete = ("""
    DELETE FROM staging_sensors
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto'
    timeformat as 'epochsecs'
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

sensorreadings_table_insert = ("""
    INSERT INTO sensorreadings (reading_id, timestamp, sensor_number, customer_id, location_id, ambient_temperature, humidity, light_level)
    SELECT e.reading_id,
        TIMESTAMP 'epoch' + e.timestamp * INTERVAL '1 Second ' AS timestamp,
        e.sensor_number,
        c.customer_id,
        s.location_id,
        e.ambient_temperature,
        e.humidity,
        e.photosensor AS light_level       
    FROM staging_events e INNER JOIN staging_sensors s ON e.sensor_number = s.sensor_number
        INNER JOIN staging_customers c ON c.customer_id = s.customer_id
""")

sensors_table_insert = ("""
    INSERT INTO sensors (sensor_number, sensor_name, description, customer_id, sku)
    SELECT ss.sensor_number,
        ss.sensor_name,
        ss.description,
        ss.customer_id,
        ss.sku
    FROM staging_sensors ss LEFT JOIN sensors s ON ss.sensor_number = s.sensor_number
    WHERE s.sensor_number IS NULL
""")

customers_table_insert = ("""
    INSERT INTO customers (customer_id, company, city, state)
    SELECT sc.customer_id,
        sc.company,
        sc.city,
        sc.state
    FROM staging_customers sc LEFT JOIN customers c ON sc.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""")

time_table_insert = ("""
    INSERT INTO time (timestamp, month, date, year, hour, minute, second, weekday)
    SELECT DISTINCT(sr.timestamp),
        EXTRACT(month from sr.timestamp) as month,
        EXTRACT(day from sr.timestamp) as date,
        EXTRACT(year from sr.timestamp) as year,
        EXTRACT(hour from sr.timestamp) as hour,
        EXTRACT(minute from sr.timestamp) as minute,
        EXTRACT(second from sr.timestamp) as second,
        EXTRACT(weekday from sr.timestamp) as weekday
    FROM sensorreadings sr LEFT JOIN time t ON sr.timestamp = t.timestamp
    WHERE t.timestamp IS NULL
""")

locations_table_insert = ("""
    INSERT INTO locations (location_id, description, latitude, longitude)
    SELECT DISTINCT ss.location_id,
        ss.location_description AS description,
        ss.latitude,
        ss.longitude
    FROM staging_sensors ss LEFT JOIN locations l ON ss.location_id = l.location_id
    WHERE l.location_id IS NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_customers_table_create, staging_sensors_table_create, sensorreadings_table_create, sensors_table_create, customers_table_create, time_table_create, locations_table_create]
drop_table_queries = [staging_events_table_drop, staging_customers_table_drop, staging_sensors_table_drop, sensorreadings_table_drop, sensors_table_drop, customers_table_drop, time_table_drop, locations_table_drop]
delete_table_queries = [staging_events_delete, staging_customers_delete, staging_sensors_delete]
copy_table_queries = [staging_events_copy, staging_customers_copy, staging_sensors_copy]
insert_table_queries = [sensorreadings_table_insert, sensors_table_insert, customers_table_insert, time_table_insert, locations_table_insert]