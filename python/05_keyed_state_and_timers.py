
import time

from pyflink.common import Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor


class PaceEvents(MapFunction):
    def map(self, value):
        time.sleep(1.0)
        return value


class PerKeyCounter(KeyedProcessFunction):
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor(
            "event-count",
            Types.LONG()
        )
        self.count_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        current = self.count_state.value()
        if current is None:
            current = 0
            timer_at = (
                ctx.timer_service().current_processing_time() + 3_000
            )
            ctx.timer_service().register_processing_time_timer(timer_at)

        updated = current + 1
        self.count_state.update(updated)

        return [f"EVENT key={ctx.get_current_key()} count={updated}"]

    def on_timer(self, timestamp, ctx):
        return [
            f"TIMER FIRED key={ctx.get_current_key()} "
            f"timestamp={timestamp} count={self.count_state.value()}"
        ]


def main():
    config = Configuration()
    config.set_integer("python.fn-execution.bundle.size", 1)

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    raw_events = env.from_collection(
        collection=["A", "B", "A", "A", "B", "A"],
        type_info=Types.STRING(),
    )

    paced_events = raw_events.map(
        PaceEvents(),
        output_type=Types.STRING()
    )

    results = (
        paced_events
        .key_by(lambda key: key, key_type=Types.STRING())
        .process(
            PerKeyCounter(),
            output_type=Types.STRING()
        )
    )

    results.print("STATE AND TIMER")
    env.execute("pyflink-keyed-state-and-timer-demo")


if __name__ == "__main__":
    main()
