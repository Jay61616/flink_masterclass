from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment
from pyflink.table.expressions import col

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    table_env = StreamTableEnvironment.create(env)

    orders = table_env.from_elements(
        [
            (1001, "C001", 150.50, "PAID"),
            (1002, "C002", 75.00, "PAID"),
            (1003, "C003", 230.00, "PENDING"),
            (1004, "C001", 499.00, "PAID"),
        ],
        DataTypes.ROW(
            [
                DataTypes.FIELD("order_id", DataTypes.BIGINT()),
                DataTypes.FIELD("customer_id", DataTypes.STRING()),
                DataTypes.FIELD("amount", DataTypes.DOUBLE()),
                DataTypes.FIELD("status", DataTypes.STRING()),
            ]
        ),
    )

    eligible_orders = orders.filter(
        (col("status") == "PAID") & (col("amount") >= 100.0)
    )

    customer_totals = eligible_orders.group_by(
        col("customer_id")
    ).select(
        col("customer_id"), col("amount").sum.alias("total_amount")
    )

    # A bounded example uses TableResult.print() to display the result rows.
    customer_totals.execute().print()


if __name__ == "__main__":
    main()
