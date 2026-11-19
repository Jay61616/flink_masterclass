import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


def parse_order(raw_json):
    try:
        payload = json.loads(raw_json)

        return (
            str(payload["customer_id"]),
            float(payload["amount"])
        )

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        print(f"Invalid order message: {raw_json}. Error: {e}")
        return None

def to_json(row):
    return json.dumps(
        {
            "customer_id": row[0],
            "running_paid_amount": round(float(row[1]), 2),
        }
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_topics("orders-pyflink")
        .set_group_id("pyflink-paid-orders")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_events = env.from_source(
        source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="orders-pyflink-source",
    )

    parsed = raw_events.map(
        parse_order,
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]),
    )

    parsed = parsed.filter(lambda order: order is not None)

    paid_orders = parsed.filter(lambda order: order[1] >= 100.0)

    running_totals = paid_orders.key_by(
        lambda order: order[0], key_type=Types.STRING()
    ).sum(1)

    output_json = running_totals.map(to_json, output_type=Types.STRING())

    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic("paid-order-totals-pyflink")
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_record_serializer(record_serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    output_json.sink_to(sink).name("paid-order-totals-sink")
    env.execute("pyflink-kafka-running-total-pipeline")


if __name__ == "__main__":
    main()
