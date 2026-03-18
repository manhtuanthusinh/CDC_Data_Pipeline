# CDC_Data_Pipeline (MySQL -> ClickHouse)
A high-performance Change Data Capture (CDC) pipeline designed for Industrial IoT (IIoT) and Predictive Maintenance scenarios. This project synchronizes transactional data from MySQL to a ClickHouse OLAP warehouse in real-time using Debezium, Apache Spark, Apache Kafka.

## Architecture
```bash
# data flow
  MySQL (OLTP)
     │
     │ binlog
     ▼
  Debezium CDC
     │
     ▼
   Kafka
     │
     ▼
  Spark Streaming
     │
     ▼
  Data Warehouse Clickhouse 
```

+ Ingestion (OLTP): MySQL as the source with binlog enabled

+ Capture: Debezium reads the MySQL binlog and publishes row-level change events to Kafka.

+ Processing: Spark Streaming consumes Kafka topics, performs transformations (cleansing, schema enforcement)

+ Destination: Data is sunk into ClickHouse for analytical queries.

## TechStack
- Source: MySQL 8.0
- CDC Engine: Debezium
- Messaging: Apache Kafka
- Processing: Apache Spark
- Sink: ClickHouse
- Others: Ubuntu 24.04 LTS, Docker Engine 

## Getting Started

### Environment and compatibility
- This project is expected to be cross-platform and runs via Docker
-  Well tested on Ubuntu 24.04 LTS
- Not fully tested (but expected to work) on MacOS and Windows (WSL2) 



### 1. Prequisites
- Docker Engine
- Java 8+
- Python 3.9+
- uv (for manage python packages and projects, written in RUST) (read the guides in LINK and install ) -> [LINK](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer)

### 2. Start the Infrastructure
#### Step 1: Install dependencies
``` bash
pip install -r requirements.txt

```


#### Step 2: Start services with Docker
``` bash
cd docker
docker compose -d
```
This will start all the services needed.

#### Step 3: check if the ```init_db.sql``` runs automatically: 
```
docker exec -it mysql_cdc mysql -u root -p inventory 
```
#### once get inside MySQL
``` SQL
SHOW TABLES;
```
if you see the table name ```order```, it works 😊😊😊

#### Step 4: Create Kafka topics
create kafka topic : commands in ```scripts/commands.txt```

### Step 5: Run application
```
uv run src/main.py
```

### Step 6: Open terminals to test pipeline

### Terminal 1: MySQL (insert data)
``` Bash
docker exec -it mysql_cdc mysql -u root -p inventory
```
```SQL
INSERT INTO orders (customer_id, product_name, price, status) VALUES
(106, 'Industrial Sensor B2', 155.00, 'ACTIVE'),
(107, 'Vibration Monitor Z', 305.50, 'PENDING'),
(108, 'Thermal Camera Pro', 910.00, 'ACTIVE'),
(109, 'Pressure Valve XL', 60.00, 'SHIPPED'),
(110, 'Gateway Controller Plus', 135.00, 'ACTIVE');
```

### Terminal 2: ClickHouse (verify data)
``` Bash
docker exec -it clickhouse_cdc clickhouse-client --user default --password 1111
```
``` SQL
SELECT * FROM orders_warehouse;
```

### Terminal 3: Application running
Keep this running 
``` Bash
uv run src/main.py
```
You should see logs showing data flowing through the pipeline.