package org.itpu;

import org.apache.spark.SparkConf;
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

    public void run() throws IOException {
        System.out.println("Started Spark Streaming! Give us a sec...");


        String csvHeaderInitial = "receipt_id,id,franchise_id,franchise_name,country,city,lat,lng,avg_tmpr_c,total_cost,discount,real_total_cost,items_count,order_size,restaurant_franchise_id,date,erroneous_orders,tiny_orders,small_orders,medium_orders,large_orders,most_popular_order_type";
        String csvHeaderFinal = "receipt_id,id,franchise_id,franchise_name,country,city,lat,lng,avg_tmpr_c,total_cost,discount,real_total_cost,items_count,order_size,restaurant_franchise_id,date,erroneous_orders,tiny_orders,small_orders,medium_orders,large_orders,most_popular_order_type, promo_cold_drinks";

        String[] columnNames = csvHeaderInitial.split(",");
        StructField[] fields = new StructField[columnNames.length];

        for(int i = 0; i < columnNames.length; i++) {
            fields[i] = DataTypes.createStructField(columnNames[i], DataTypes.StringType, true);
        }

        StructType schema = new StructType(fields);



        File outPutFile = new File(OUTPUT_DIRECTORY);
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(outPutFile, true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if((!outPutFile.exists()) || (outPutFile.length() == 0)) {
            try {
                outPutFile.createNewFile();
                writer.write(csvHeaderFinal);
                writer.newLine();
                writer.close();
                System.out.println("New output file created!");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("Output file already exists!");
        }



        // Spark Streaming Context
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("DataStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // DStream that monitors input directory
        JavaDStream<String> lines = jssc.textFileStream(INPUT_DIRECTORY);

        // Process CSV lines without header
        JavaDStream<String[]> csvInputData = lines.filter(line -> !line.startsWith("receipt_id,id,franchise_id,franchise_name"))
                .map(line -> line.split(","));

        // Process CSV data
        csvInputData.foreachRDD(rdd -> {
            SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
            Dataset<Row> csvDataFrame = spark.createDataFrame(rdd.map(row -> RowFactory.create((Object[]) row)), schema);

            if (!csvDataFrame.isEmpty()) {
                csvDataFrame = csvDataFrame.withColumn("avg_tmpr_c", col("avg_tmpr_c").cast(DataTypes.DoubleType))
                        .withColumn("promo_cold_drinks", when(col("avg_tmpr_c").gt(25.0), true).otherwise(false));

                writeStreamingOutput(OUTPUT_DIRECTORY, csvDataFrame);

                System.out.println("Processed dataset is");
                csvDataFrame.show();
            } else {
                System.out.println("For now the input stream is empty");
            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Streaming operation was successful. Data processed and stored in the output folder.");
        System.out.println("Done! :)");
    }

    public static void main(String[] args) throws IOException {
        new SparkStreaming().run();
    }

    private void writeStreamingOutput(String outputDirectory, Dataset<Row> csvDataFrame) throws IOException {
        File file = new File(outputDirectory);
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));


        System.out.println("Writing data");

        Dataset<String> csvStrings = csvDataFrame.map(
                (MapFunction<Row, String>) row -> Arrays.stream(row.schema().fieldNames())
                        .map(fieldName -> {
                            Object value = row.getAs(fieldName);
                            return (value==null) ? "" : value.toString();
                        })
                        .collect(Collectors.joining(",")), Encoders.STRING());

        for (String line : csvStrings.collectAsList()) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
    }
}
