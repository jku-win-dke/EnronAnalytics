package at.jku.dke.dwh.enron;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SimpleSparkStreamingApp {
    private static final class StateAccumulatorFunction implements Function2<List<Integer>, Optional<Integer>, Optional<Integer>>, Serializable {
        @Override
        public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
            Integer newState =
                    state.isPresent() ?
                            state.get() + values.stream().mapToInt(n -> n.intValue()).sum() :
                            values.stream().mapToInt(n -> n.intValue()).sum();
            return Optional.of(newState);
        }
    }

    public static void main(String[] args) {
        // EmailGenerator writes a CSV file every 10 seconds into the output directory.
        // The EmailGenerator randomly copies one of the files from data into the output directory.
        EmailGenerator.generate();

        SimpleSparkStreamingApp app = new SimpleSparkStreamingApp();

        app.start();
    }

    private void start() {
        SparkConf conf = new SparkConf()
                .setAppName("Simple Spark streaming app with DStreams")
                .setMaster("local[*]");

        try (
            // We obtain a streaming context. The second argument specifies
            // that the time intervals at which Spark Streaming splits the
            // stream into mini batches. Here, the stream is split up into
            // batches of 5 seconds worth of data.
            //
            // Note: We could also use StreamingContext and DStream instead
            // of the Java variants but since we use Java we use the Java-friendly
            // versions of those classes.
            JavaStreamingContext ssc =
                new JavaStreamingContext(conf, Durations.seconds(10))
        ) {
            // We obtain a discretized stream reading all the new text files
            // in a specified directory within a certain time interval.
            JavaDStream<Email> emailStream =
                ssc.textFileStream("./output").map(
                    line -> {
                        String[] columns = line.split(",");

                        String from = columns[0];
                        String to = columns[1];
                        LocalDateTime timestamp = null;
                        String body = columns[3];

                        return new Email(from, to, timestamp, body);
                    }
                );

            // We obtain a pair stream, mapping every input Email object to a key-value pair (2-Tuple) with
            // sender's e-mail address (string) and the number of words in the body of the e-mail.
            // For example, the emails
            //
            // schuetz@dke.uni-linz.ac.at,schrefl@dke.uni-linz.ac.at,2016-12-14T18:46,Hallo Michael!   Wie geht es dir?
            // schuetz@dke.uni-linz.ac.at,neumayr@dke.uni-linz.ac.at,2016-12-14T18:47,Hallo Bernd!   Was geht ab?
            // schrefl@dke.uni-linz.ac.at,schuetz@dke.uni-linz.ac.at,2016-12-14T18:48,Mir geht es gut.   Danke der Nachfrage.
            //
            // translate into the 2-tuples
            //
            // (schuetz@dke.uni-linz.ac.at, 6)
            // (schuetz@dke.uni-linz.ac.at, 5)
            // (schrefl@dke.uni-linz.ac.at, 7)
            //
            // We then reduce by the key, i.e., the first column, adding up the values (second column, word count) per
            // key, to obtain the following:
            // (schuetz@dke.uni-linz.ac.at, 11)
            // (schrefl@dke.uni-linz.ac.at, 7)
            JavaPairDStream<String,Integer> wordCountPerSenderStream =
                    emailStream.mapToPair(
                        email ->
                            new Tuple2<String,Integer>(email.getFrom(), email.getBody().replaceAll("  ", " ").split(" ").length)
                    ).reduceByKey(
                            (count1,count2) -> count1 + count2
                    );

            wordCountPerSenderStream.repartition(1).print();

            // Obtain word count in a sliding window of 30 seconds.
            // Batches from the last 30 seconds are combined into one batch.
            // The window moves in 10 second intervals.
            JavaPairDStream<String,Integer> windowedWordCountPerSenderStream =
                wordCountPerSenderStream.reduceByKeyAndWindow(
                        (i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10)
                );

            windowedWordCountPerSenderStream.repartition(1).print();

            // get the message count per sender
            JavaPairDStream<String,Integer> messageCountPerSenderStream =
                    emailStream.mapToPair(
                            email ->
                                    new Tuple2<String,Integer>(email.getFrom(), 1)
                    ).reduceByKey(
                            (count1,count2) -> count1 + count2
                    );

            JavaPairDStream<String,Integer> windowedMessageCountPerSenderStream =
                    messageCountPerSenderStream.reduceByKeyAndWindow(
                            (i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10)
                    );

            windowedMessageCountPerSenderStream.repartition(1).print();

            // Join the streams so that we get for each sender the number of messages sent within
            // the window along with the number of words over all messages.
            // This can then be used to compute the average number of words per mail for a sender.
            JavaPairDStream<String, Tuple2<Integer, Integer>> join =
                    windowedMessageCountPerSenderStream.join(windowedWordCountPerSenderStream);

            join.print();


            // Compute the average word count per message and sender over the joined stream.
            JavaDStream<Tuple2<String, Double>> averageWordCountPerMessagePerSender =
                    join.map(o -> new Tuple2<String, Double>(o._1, o._2._2.doubleValue() / o._2._1.doubleValue()));

            averageWordCountPerMessagePerSender.print();

            // == State ==
            // see: https://spark.apache.org/docs/3.0.0/streaming-programming-guide.html#checkpointing
            ssc.checkpoint("./checkpoints");

            // for each key (sender) keep a state that is updated with each batch
            JavaPairDStream<String, Integer> stateStream = wordCountPerSenderStream.updateStateByKey(
                    new StateAccumulatorFunction()
            );

            stateStream.print();

            // now use the state to find top sender by number of words
            stateStream.mapToPair(
                    // in Java we have to swap the values of the pair
                    // so that the value becomes the "key"
                    pair -> pair.swap()
            ).transform(
                    // then we can sort by key, get an index (0, 1, 2, ...)
                    // and show only the first
                    rdd -> rdd.sortByKey(false).map(t -> t._2)
                              .zipWithIndex().filter(t -> t._2 <= 0)
                              .map(pair -> pair._1)
            ).print();


            ssc.start();

            try {
                // run the stream processing until the application is terminated
                // from outside.
                ssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
