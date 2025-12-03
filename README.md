# Fraud streamhouse with Fluss, Flink and Iceberg


Disclaimer:
I created this repository to experiment with Apache Fluss (Incubating). Please note that the cluster sizing and security configuration are intended for exploration only and are not production-ready.


Fluss/Flik required JARs:
https://fluss.apache.org/docs/streaming-lakehouse/integrate-data-lakes/iceberg/


Fraud Job JAR packaging cmd:
mvn package -f pom.xml


Flink Job submit parameters:
--parallelism 2 --fluss.bootstrap.servers coordinator-server:9122 --datalake.format iceberg --datalake.iceberg.type rest --datalake.iceberg.warehouse s3://fluss/data --datalake.iceberg.uri http://rest:8181 --datalake.iceberg.s3.endpoint http://minio:9000 --datalake.iceberg.s3.path-style-access true


Docker commands:
docker-compose down -v
docker-compose up --build


Flink SQL client commands:
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',   
    'bootstrap.servers' = 'coordinator-server:9122'
);
USE CATALOG fluss_catalog;
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
select * from fraud;
select * from fraud$lake;





