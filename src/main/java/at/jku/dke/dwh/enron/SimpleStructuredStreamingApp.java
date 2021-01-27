package at.jku.dke.dwh.enron;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SimpleStructuredStreamingApp {

    public static void main(String[] args) {
        // EmailGenerator writes a CSV file every 10 seconds into the output directory.
        // The EmailGenerator randomly copies one of the files from data into the output directory.
        EmailGenerator.generate();

        SimpleStructuredStreamingApp app = new SimpleStructuredStreamingApp();

        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Simple structured streaming with sliding window")
                .master("local")
                .getOrCreate();


        // Create a schema for the CSV
        StructType schema = DataTypes.createStructType(
                new StructField[] {
                        DataTypes.createStructField(
                                "From",
                                DataTypes.StringType,
                                false
                        ),
                        DataTypes.createStructField(
                                "To",
                                DataTypes.StringType,
                                false
                        ),
                        DataTypes.createStructField(
                                "Timestamp",
                                DataTypes.TimestampType,
                                false
                        ),
                        DataTypes.createStructField(
                                "Body",
                                DataTypes.StringType,
                                true
                        )
                }
        );

        // Open a stream; the stream "listens" to the specified directory
        Dataset<Row> df = spark
                .readStream()
                .format("csv")
                .option("header", false)
                .schema(schema)
                .load("./output")
                .withWatermark("Timestamp", "10 minutes"); // this is the event timestamp (watermark)

        // aggregate by window, show the sum of words of e-mails in a given time window
        Dataset<Row> windowedDf = df.groupBy(
                functions.window(df.col("Timestamp"), "30 seconds", "10 seconds")
        ).agg(functions.sum(functions.size(functions.split(df.col("Body"), " ")))); // compute the sum of words in the body of the e-mails in the time window


        try {
            StreamingQuery query = windowedDf.writeStream()
                    .outputMode(OutputMode.Update())
                    .format("console")
                    .option("truncate", "false")
                    .start();

            // run the stream processing until the application is terminated
            // from outside.
            query.awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
