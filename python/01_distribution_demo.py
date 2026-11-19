import socket

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction


class ReportTaskManager(MapFunction):

    def open(self, runtime_context):
        self.subtask_index = runtime_context.get_index_of_this_subtask()
        self.parallelism = runtime_context.get_number_of_parallel_subtasks()

    def map(self, value):
        return (
            value,
            socket.gethostname(),
            self.subtask_index,
            self.parallelism,
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    source = env.from_collection(
        collection=list(range(1, 41)),
        type_info=Types.LONG(),
    ).set_parallelism(1)

    distributed = (
        source.rebalance()
        .map(ReportTaskManager(),
            output_type=Types.TUPLE(
                [Types.LONG(), Types.STRING(), Types.INT(), Types.INT()]

            ),
        )
        .name("report-taskmanager-and-subtask")
        .set_parallelism(4)
    )

    distributed.print("DISTRIBUTED RECORD")
    env.execute("pyflink-two-taskmanager-distribution")


if __name__ == "__main__":
    main()