package org.itpu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class SparkStreaming {

    private final String INPUT_DIRECTORY = "./streamingfiles/input";
    private final String OUTPUT_DIRECTORY = "./streamingfiles/output/result.csv";

    // Define CSV header strings
    private final String CSV_HEADER_INITIAL = "receipt_id,id,franchise_id,franchise_name,country,city,lat,lng,avg_tmpr_c,total_cost,discount,real_total_cost,items_count,order_size,restaurant_franchise_id,date,erroneous_orders,tiny_orders,small_orders,medium_orders,large_orders,most_popular_order_type";
    private final String CSV_HEADER_FINAL = "receipt_id,id,franchise_id,franchise_name,country,city,lat,lng,avg_tmpr_c,total_cost,discount,real_total_cost,items_count,order_size,restaurant_franchise_id,date,erroneous_orders,tiny_orders,small_orders,medium_orders,large_orders,most_popular_order_type, promo_cold_drinks";

    public void run() throws IOException {
        System.out.println("Started Spark Streaming! Give us a sec...");

        // Check and create output file if not exists
        createOutputFileIfNotExists();

        // Spark Streaming Context setup
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("DataStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // DStream to monitor input directory
        JavaDStream<String> lines = jssc.textFileStream(INPUT_DIRECTORY);

        // Process CSV lines without header
        JavaDStream<String[]> csvInputData = lines.filter(line -> !line.startsWith("receipt_id,id,franchise_id,franchise_name"))
                .map(line -> line.split(","));

        // Process CSV data
        csvInputData.foreachRDD(this::processCSVData);

        // Start streaming context
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Streaming operation completed
        System.out.println("Streaming operation was successful. Data processed and stored in the output folder.");
        System.out.println("Done! :)");
    }

    public static void main(String[] args) throws IOException {
        new SparkStreaming().run();
    }


    // Method to create output file if not exists
    private void createOutputFileIfNotExists() {
        File outputFile = new File(OUTPUT_DIRECTORY);
        if ((!outputFile.exists()) || (outputFile.length() == 0)) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true))) {
                writer.write(CSV_HEADER_FINAL);
                writer.newLine();
                System.out.println("New output file created!");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("Output file already exists!");
        }
    }

    // Method to define schema based on CSV header
    private StructType getSchema() {
        String[] columnNames = CSV_HEADER_INITIAL.split(",");
        StructField[] fields = new StructField[columnNames.length];

        for (int i = 0; i < columnNames.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames[i], DataTypes.StringType, true);
        }

        return new StructType(fields);
    }


    // Method to process CSV data
    private void processCSVData(JavaRDD<String[]> rdd) throws IOException {
        SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();

        // Create DataFrame from RDD and apply schema
        Dataset<Row> csvDataFrame = spark.createDataFrame(rdd.map(row -> RowFactory.create((Object[]) row)), getSchema());

        if (!csvDataFrame.isEmpty()) {
            // Process DataFrame: cast temperature column, add promo cold drinks column
            csvDataFrame = csvDataFrame.withColumn("avg_tmpr_c", col("avg_tmpr_c").cast(DataTypes.DoubleType))
                    .withColumn("promo_cold_drinks", when(col("avg_tmpr_c").gt(25.0), true).otherwise(false));

            // Write DataFrame to output directory
            writeStreamingOutput(OUTPUT_DIRECTORY, csvDataFrame);

            // Display processed DataFrame
            System.out.println("Processed dataset is");
            csvDataFrame.show();
        } else {
            // If input stream is empty
            System.out.println("For now the input stream is empty");
        }
    }

    // Method to write DataFrame to output directory
    private void writeStreamingOutput(String outputDirectory, Dataset<Row> csvDataFrame) throws IOException {
        File file = new File(outputDirectory);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

        System.out.println("Writing data");

        // Convert DataFrame to CSV strings
        Dataset<String> csvStrings = csvDataFrame.map(
                (MapFunction<Row, String>) row -> Arrays.stream(row.schema().fieldNames())
                        .map(fieldName -> {
                            Object value = row.getAs(fieldName);
                            return (value==null) ? "" : value.toString();
                        })
                        .collect(Collectors.joining(",")), Encoders.STRING());

        // Write CSV strings to file
        for (String line : csvStrings.collectAsList()) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
    }
}
