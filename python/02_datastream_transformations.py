from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    sentences = env.from_collection(
        collection=[
            "Flink processes streaming events",
            "Flink supports stateful streaming",
            "Python programs use the DataStream API",
            "Streaming systems process continuous events",
        ],
        type_info=Types.STRING(),
    )

    # flat_map: one sentence can produce multiple words.
    words = sentences.flat_map(
        lambda sentence: sentence.lower().split(),
        output_type=Types.STRING(),
    )

    # filter: retain words longer than three characters.
    useful_words = words.filter(lambda word: len(word) > 3)

    # map: convert each word into a (word, 1) pair.
    word_pairs = useful_words.map(lambda word: (word, 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
    )

    # key_by redistributes equal words to the same keyed partition.
    # sum(1) maintains a running count in the second tuple field.
    counts = word_pairs.key_by(lambda pair: pair[0], key_type=Types.STRING()).sum(1)
    counts.print("RUNNING WORD COUNT")

    env.execute("pyflink-datastream-transformations")


if __name__ == "__main__":
    main()
