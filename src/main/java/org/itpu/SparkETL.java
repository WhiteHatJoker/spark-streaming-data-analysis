package org.itpu;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;


public class SparkETL {

    // 1) Set paths to source files
    private final String RESTAURANTS_CSV_PATH = "src/main/resources/restaurant_csv";
    private final String WEATHER_CSV_PATH = "src/main/resources/weather_csv";
    private final String OUTPUT_PATH = "output"; //path to output folder


    /**
     * 3) Load the files' content to spark datasets
     *
     * @param spark       spark session
     * @param pathToFiles path to files
     * @return dataset
     */
    public Dataset<Row> readDatasetFromCsv(SparkSession spark, String pathToFiles) {
        return spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load(pathToFiles);
    }

    public Dataset<Row> formatData(Dataset<Row> unformattedData, String columnToDrop) {
        // Filter out only data for 2022
        Dataset<Row> yearFiltered = unformattedData.filter(col(columnToDrop).startsWith("2022"));
        // Convert datetime into date leaving the timezone
        Dataset<Row> dateChanged = yearFiltered.withColumn("date", col(columnToDrop).cast("date"));

        // Round latitude and longtitude to 2 decimals
        Dataset<Row> roundedLocation = dateChanged
              .withColumn("lat", round(col("lat"), 2))
              .withColumn("lng", round(col("lng"), 2));
        // Drop the date column
        return roundedLocation.drop(columnToDrop);
    }


    public void run() {
        System.out.println("Started Spark ETL! Give us a sec...");

        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("DataPrep")
                .master("local[*]")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        // Step 1: Read the CSV file
        System.out.println("Reading CSV...");
        Dataset<Row> restaurants = readDatasetFromCsv(spark, RESTAURANTS_CSV_PATH);
        Dataset<Row> weather = readDatasetFromCsv(spark, WEATHER_CSV_PATH);

        // Step 2: Cleanse, filter and format data
        System.out.println("Formatting data...");
        Dataset<Row> formattedRestaurants = formatData(restaurants, "date_time");
        Dataset<Row> formattedWeather = formatData(weather, "wthr_date");

        // Step 3: Join on date and coordinates
        Dataset<Row> dfJoined = formattedRestaurants.as("r").join(formattedWeather.as("w"),
                col("r.date").equalTo(col("w.date"))
                        .and(col("r.lat").equalTo(col("w.lat")))
                        .and(col("r.lng").equalTo(col("w.lng"))),
                "inner");

        // Removing duplicate columns
        Dataset<Row> curatedDataset = dfJoined
                .drop(formattedWeather.col("date"))
                .drop(formattedWeather.col("lat"))
                .drop(formattedWeather.col("lng"))
                .drop(formattedWeather.col("city"))
                .drop(formattedWeather.col("country"));


        // Step 6: Temperature filter
        Dataset<Row> dfFiltered = curatedDataset.filter(col("avg_tmpr_c").gt(0));

        // Step 7: Calculate fields
        Dataset<Row> dfCalc = dfFiltered
                .withColumn("real_total_cost", col("total_cost").minus(col("total_cost").multiply(col("discount"))));

        // Step 4: Output the result schema and result itself
        System.out.println("Here's the schema:");
        curatedDataset.printSchema();
        System.out.println("Here's the data:");
        curatedDataset.show();


        // Step 5: Save the result to a new CSV file
        System.out.println("Writing final output CSV file...");
        curatedDataset.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(OUTPUT_PATH);

        // Close the session
        spark.stop();
        System.out.println("Operation was successful. You can find the CSV files in the output folder.");
        System.out.println("Done! :)");



//
//        // Order Size UDF
//        Encoder<String> stringEncoder = ExpressionEncoder.javaBean(String.class);
//        dfCalc = dfCalc.withColumn("order_size", functions.callUDF("orderSizeUDF", col("items_count"), stringEncoder));
//
//        // Step 8: Order type aggregates
//        WindowSpec window = Window.partitionBy("restaurant_franchise_id", "date")
//                .orderBy(dfCalc.col("large_orders").desc(),
//                        dfCalc.col("medium_orders").desc()
//                        // ... (Add other orders)
//                );
//
//        Dataset<Row> dfAggr = dfCalc.groupBy("restaurant_franchise_id", "date")
//                .agg(count(when(col("order_size").equalTo("Erroneous"), true)).alias("erroneous_orders"),
//                        count(when(col("order_size").equalTo("Tiny"), true)).alias("tiny_orders"),
//                        count(when(col("order_size").equalTo("Small"), true)).alias("small_orders"),
//                        count(when(col("order_size").equalTo("Medium"), true)).alias("medium_orders"),
//                        count(when(col("order_size").equalTo("Large"), true)).alias("large_orders"))
//                .withColumn("most_popular_order_type", first("order_size").over(window));
//
//        // Step 9: Store the result (adjust path)
//        dfAggr.write().format("csv").option("header", true).mode("overwrite").save("path/to/result_data");
    }

    public static void main(String[] args) {
        new SparkETL().run();
    }
    private static  MapFunction<Integer, String> orderSizeUDF = (Integer items) -> {
        if (items == null) return null;
        else if (items <= 0) return "Erroneous";
        else if (0 < items && items <= 1) return "Tiny";
            // ... add other conditions
        else return "Large";
    };

}
