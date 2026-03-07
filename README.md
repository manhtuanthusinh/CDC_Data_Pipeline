# CDC_Data_Pipeline

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
     │
     | ------  Data Lake (delta/iceberg)
     |
     ▼
BI Dashboard
```