import time

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows


class PaceEvents(MapFunction):
    """Pace a finite teaching source so periodic watermarks are visible."""

    def map(self, event):
        time.sleep(0.25)
        return event



class SensorTimestampAssigner(TimestampAssigner):
    """Extract event time from the second field of each sensor reading."""

    def extract_timestamp(self, event, record_timestamp):
        return event[1]


def combine_readings(left, right):
    return (
        left[0],
        max(left[1], right[1]),
        left[2] + right[2],
        left[3] + right[3],
    )


def format_average(row):
    sensor_id, max_event_ms, temperature_sum, reading_count = row
    average = temperature_sum / reading_count
    return (
        f"sensor={sensor_id}, max_event_ms={max_event_ms}, "
        f"count={reading_count}, average={average:.2f}"
    )



def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(100)

    readings = [
        ("sensor-A", 1_000, 22.0, 1),
        ("sensor-A", 8_000, 26.0, 1),
        ("sensor-B", 5_000, 19.0, 1),
        ("sensor-A", 13_000, 28.0, 1),
        ("sensor-A", 4_000, 24.0, 1),
        ("sensor-A", 16_000, 30.0, 1),
    ]

    source = env.from_collection(
        collection=readings,
        type_info=Types.TUPLE(
            [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.INT()]
        ),
    )

    paced = source.map(
        PaceEvents(),
        output_type=Types.TUPLE(
            [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.INT()]
        ),
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(SensorTimestampAssigner())
    )

    timestamped = paced.assign_timestamps_and_watermarks(watermark_strategy)

    windowed = (
        timestamped
        .key_by(lambda event: event[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .reduce(combine_readings)
    )

    windowed.map(
        format_average,
        output_type=Types.STRING()
    ).print("10-SECOND WINDOW")

    env.execute("pyflink-event-time-window-demo")


if __name__ == "__main__":
    main()