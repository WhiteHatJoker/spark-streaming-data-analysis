package org.itpu;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;

public class SparkStreaming {

    private final String INPUT_DIRECTORY = "streamingfiles/input";
    private final String OUTPUT_DIRECTORY = "streamingfiles/output/result.csv";

    public void run() {
        System.out.println("Started Spark Streaming ETL! Give us a sec...");

        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("DataStreaming")
                .master("local[*]")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), new Duration(5000));

        // Input stream from directory
        JavaDStream<Row> inputData = ssc.textFileStream(INPUT_DIRECTORY)
                .map(line -> {
                    String[] columns = line.split(",");
                    if (columns[0].equals("avg_tmpr_c")) { // Check for header
                        return null; // Skip the header row
                    }
                    double avgTemp = Double.parseDouble(columns[8]); // Assuming 'avg_tmpr_c' position
                    boolean promoColdDrinks = avgTemp > 25.0;

                    // Construct new Row with all original columns + the calculated field
                    return RowFactory.create(columns[0], columns[1], columns[2],columns[3], columns[4],columns[5],
                            columns[6], columns[7], columns[8], columns[9], columns[10], columns[11], columns[12], columns[13],
                            columns[14], columns[15], columns[16], columns[17], columns[18], columns[19], columns[20], columns[21],promoColdDrinks);
                });

        // Output Logic (Single CSV)
//        inputData.foreachRDD(rdd -> {
//            Dataset<Row> sampleDF = null;
//            if (rdd.isEmpty()) {  // Check for the first RDD
//                // Infer schema from a sample
//                sampleDF = spark.read().option("header", true).option("inferSchema", true).csv(INPUT_DIRECTORY);
//
//                // Use the inferred schema
//                Dataset<Row> df = spark.createDataFrame(rdd.rdd(), sampleDF.schema());
//                df.write().mode("overwrite").option("header", true).csv(OUTPUT_DIRECTORY);
//            } else {
//                Dataset<Row> df = spark.createDataFrame(rdd.rdd(), sampleDF.schema()); // Reuse inferred schema
//                df.write().mode("append").option("header", false).csv(OUTPUT_DIRECTORY);
//            }
//        });

        inputData.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {  // Check if a file has arrived
                // Infer schema from the data
                Dataset<Row> df = spark.read().option("header", true).option("inferSchema", true).csv(INPUT_DIRECTORY);

                // Process and write output
                df.write().mode("append").option("header", false).csv(OUTPUT_DIRECTORY);
            }
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Close the session
        spark.stop();
        System.out.println("Streaming operation was successful. Data processed and stored in the output folder.");
        System.out.println("Done! :)");
    }

    public static void main(String[] args) {
        new SparkStreaming().run();
    }
}
