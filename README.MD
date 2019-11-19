## 0. Create Aurora PostgreSQL Serverless Cluster

### 0-1. Create DB Subnet Group and Cluster
```
aws rds create-db-subnet-group \
--db-subnet-group-name database-vpc-subnet-group \
--db-subnet-group-description database_vpc_subnet_group \
--subnet-ids <subnet_id_1> <subnet_id_2> <subnet_id_3>

aws rds create-db-cluster --db-cluster-identifier sls-postgres \
--engine aurora-postgresql --engine-version 10.7 --engine-mode serverless \
--scaling-configuration MinCapacity=2,MaxCapacity=4 \
--enable-http-endpoint \
--master-username <username> --master-user-password <password> \
--enable-http-endpoint \
--db-subnet-group-name database-vpc-subnet-group \
--vpc-security-group-ids <security-group-id>

```

### 0-2. Create secret
```
aws secretsmanager create-secret --name sls-postgres-secret --secret-string "file://creds-sls-postgres.json"
```

### 0-3. Create Database 'triad'
```
aws rds-data execute-statement --resource-arn "arn:aws:rds:ap-northeast-1:042083552617:cluster:sls-postgres" --secret-arn "arn:aws:secretsmanager:ap-northeast-1:042083552617:secret:sls-postgres-secret-fNbs6H" --sql "SELECT datname FROM pg_database" --database "postgres"

aws rds-data execute-statement --resource-arn "arn:aws:rds:ap-northeast-1:042083552617:cluster:sls-postgres" --secret-arn "arn:aws:secretsmanager:ap-northeast-1:042083552617:secret:sls-postgres-secret-fNbs6H" --sql "create DATABASE triad" --database "postgres"
```

### 0-4. Create Table 'Tumbling'
```
aws rds-data execute-statement --resource-arn "arn:aws:rds:ap-northeast-1:042083552617:cluster:sls-postgres" --secret-arn "arn:aws:secretsmanager:ap-northeast-1:042083552617:secret:sls-postgres-secret-fNbs6H" --sql "create table Tumbling(RAILWAY_CLASS varchar(20), RAILWAY_CLASS_COUNT bigint, WINDOW_START timestamp, WINDOW_END timestamp, PRIMARY KEY(RAILWAY_CLASS, WINDOW_START, WINDOW_END))" --database "triad"
```

### 0-5. Create Table 'Sliding'
```
aws rds-data execute-statement --resource-arn "arn:aws:rds:ap-northeast-1:042083552617:cluster:sls-postgres" --secret-arn "arn:aws:secretsmanager:ap-northeast-1:042083552617:secret:sls-postgres-secret-fNbs6H" --sql "create table Sliding(RAILWAY_CLASS varchar(20), RAILWAY_CLASS_COUNT bigint, EVENT_TIME timestamp, PRIMARY KEY(RAILWAY_CLASS))" --database "triad"
```

### 0-6. Create Table 'Hopping'
```
aws rds-data execute-statement --resource-arn "arn:aws:rds:ap-northeast-1:042083552617:cluster:sls-postgres" --secret-arn "arn:aws:secretsmanager:ap-northeast-1:042083552617:secret:sls-postgres-secret-fNbs6H" --sql "create table Hopping(RAILWAY_CLASS varchar(20), RAILWAY_CLASS_COUNT bigint, WINDOW_START timestamp, WINDOW_END timestamp, RECEIVED_ON timestamp, PRIMARY KEY(RAILWAY_CLASS, WINDOW_START, WINDOW_END))" --database "triad"
```

## 1. Create local flink-connector-kinesis repo
### 1-1. Download and Build Flink 1.9.1
[flink](https://github.com/apache/flink/archive/release-1.9.1.zip)

```
wget -qO- https://github.com/apache/flink/archive/release-1.9.1.zip | bsdtar -xf-
cd flink-release-1.9.1
mvn clean install -Pinclude-kinesis -DskipTests -pl flink-connectors/flink-connector-kinesis
```

### 1-2. Download and Build Flink 1.6.2
```
wget -qO- https://github.com/apache/flink/archive/release-1.6.2.zip | bsdtar -xf-
cd flink-release-1.6.2
mvn clean install -Pinclude-kinesis -DskipTests -pl flink-connectors/flink-connector-kinesis
```

### 1-3. Download and Build Flink 1.6.4
```
wget -qO- https://github.com/apache/flink/archive/release-1.6.4.zip | bsdtar -xf-
cd flink-release-1.6.4
mvn clean install -Pinclude-kinesis -DskipTests -pl flink-connectors/flink-connector-kinesis
```

## 2. Build this repo
```
mvn package
```

```
SHARD_ITERATOR=$(aws kinesis get-shard-iterator --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --stream-name amp_geojson --query 'ShardIterator')

aws kinesis get-records --shard-iterator $SHARD_ITERATOR --limit 10
```

---------------
## 4. References
### 4-1. AWS Official KDA Examples
#### Example: Using a Custom Sink to Write to CloudWatch Logs
https://docs.aws.amazon.com/en_pv/kinesisanalytics/latest/java/examples-cloudwatch.html

#### Example: Sliding Window
https://docs.aws.amazon.com/en_pv/kinesisanalytics/latest/java/examples-sliding.html

### 4-2. DataStream API Examples
#### Java Code Examples for org.apache.flink.streaming.api.functions.sink.SinkFunction
https://www.programcreek.com/java-api-examples/?api=org.apache.flink.streaming.api.functions.sink.SinkFunction

#### Amazon Kinesis Data Analytics Java Examples
https://github.com/aws-samples/amazon-kinesis-data-analytics-java-examples

#### Build and run streaming applications with Apache Flink and Amazon Kinesis Data Analytics for Java Applications
https://github.com/aws-samples/amazon-kinesis-analytics-taxi-consumer
https://aws.amazon.com/blogs/big-data/build-and-run-streaming-applications-with-apache-flink-and-amazon-kinesis-data-analytics-for-java-applications/

### 4-3. DataStream API Examples - Custom Aggregate Function
#### Apache FlinkとJava 8でセンサーデータをウィンドウ集計をする
https://qiita.com/masato/items/32d84f117152ea0fdb0b

#### Hands-on with Event Stream Processing Frameworks
https://medium.com/@cjolif/hands-on-with-event-stream-processing-frameworks-8be69101a1c8
https://github.com/cjolif/streaming-examples/blob/master/flink/src/main/java/Example.java

### 4-4. Table/SQL API Examples
#### Amazon Kinesis Data Analytics for Java - Leveraging the Apache Flink Table Api
https://github.com/aws-samples/amazon-kinesis-data-analytics-flinktableapi

#### Simple aggregation from a CSV
https://riptutorial.com/apache-flink/example/27901/simple-aggregation-from-a-csv

### 4-5. Custom Sink Examples
#### Writing to PostgreSQL from Apache Flink
https://tech.signavio.com/2017/postgres-flink-sink

### 4-6. Tools
#### Amazon Kinesis Data Generator
https://awslabs.github.io/amazon-kinesis-data-generator/web/help.html

#### Importing Flink into an IDE
https://ci.apache.org/projects/flink/flink-docs-stable/flinkDev/ide_setup.html

### 4-7. Flink Official Doc
#### DataStream API
https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/
https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/windows.html

#### Table/SQL API
https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/tableApi.html
https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html

### 4-8. Flink Overview
#### Flink 关系型 API 解读：Table API 与 SQL
https://www.infoq.cn/article/flink-api-table-api-and-sql

#### Java Doc
https://ci.apache.org/projects/flink/flink-docs-release-1.6/api/java/org/apache/flink/streaming/api/datastream/DataStream.html
https://ci.apache.org/projects/flink/flink-docs-release-1.6/api/java/org/apache/flink/streaming/api/datastream/KeyedStream.html
https://ci.apache.org/projects/flink/flink-docs-release-1.6/api/java/org/apache/flink/streaming/api/datastream/WindowedStream.html