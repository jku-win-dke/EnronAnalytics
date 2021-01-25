package at.jku.dke.dwh.enron;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class SimpleCSVApp {
    public static void main(String[] args) {
        SimpleCSVApp app = new SimpleCSVApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Simple CSV reader and analysis")
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
                        "Body",
                        DataTypes.StringType,
                        true
                )
            }
        );

        // Read the sample CSV file
        Dataset<Row> df = spark.read().format("csv")
                .option("header", false)
                .schema(schema)
                .load("data/email.csv");

        // Split the body by comma and return the size of the resulting array in a separate column
        df = df.withColumn("Words", split(col("Body"), " "))
               .withColumn("WordCount", size(col("Words")));

        // Print schema and contents of the dataframe
        df.printSchema();
        df.show();


        // Register the dataframe as a temp view to be able to execute SQL queries
        df.createOrReplaceTempView("Emails");

        // Compute the average word count per sender
        Dataset<Row> averageWordCount =
                spark.sql("SELECT e.From, AVG(e.WordCount) AS AverageWordCountPerSender FROM Emails e GROUP BY e.From");

        averageWordCount.show();


        // The same result can also be achieved by invoking static functions
        Dataset<Row> averageWordCountAlt =
                df.groupBy(col("From")).agg(avg(col("WordCount")));

        averageWordCountAlt.show();
    }
}
