-- Load the version-matched SQL Kafka connector and JSON format for this SQL session.
ADD JAR 'file:///opt/flink/connectors/sql/flink-sql-connector-kafka-5.0.0-2.2.jar';
ADD JAR 'file:///opt/flink/connectors/sql/flink-json-2.2.1.jar';

-- Run these statements in the Flink SQL Client after creating the Kafka topics.
-- Source: unbounded Kafka topic. event_time is parsed from the JSON payload.
CREATE TABLE order_events (
    order_id BIGINT,
    customer_id STRING,
    amount DECIMAL(10, 2),
    status STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'flink-masterclass-orders',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- Append-only output topic for qualifying paid orders.
CREATE TABLE paid_orders (
    order_id BIGINT,
    customer_id STRING,
    amount DECIMAL(10, 2),
    status STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'paid-orders',
    'properties.bootstrap.servers' = 'kafka:19092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'flink_mc_paid_orders_'
);

-- This INSERT starts a continuous streaming job; it does not finish like a bounded query.
INSERT INTO paid_orders
SELECT order_id, customer_id, amount, status, event_time
FROM order_events
WHERE status = 'PAID' AND amount >= 100.00;
