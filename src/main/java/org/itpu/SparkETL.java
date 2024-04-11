package org.itpu;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class SparkETL {

    // 1) Set paths to source files
    private final String RESTAURANTS_CSV_PATH = "src/main/resources/restaurant_csv";
    private final String WEATHER_CSV_PATH = "src/main/resources/weather_csv";
    private final String OUTPUT_PATH = "output"; //path to output folder


    /**
     * Load the files' content to spark datasets
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

    public void printSampleOutput(Dataset<Row> finalData) {
        System.out.println("Here's the schema:");
        finalData.printSchema();
        System.out.println("Here's the data:");
        finalData.show();
    }

    public Dataset<Row> calculateOrderSize(Dataset<Row> realCostCalculated) {
        Dataset<Row> dfWithOrderSize = realCostCalculated
                .groupBy("restaurant_franchise_id", "receipt_id") // Group by franchise_id and receipt_id
                .agg(first("date").alias("date"),
                        first("id").alias("id"), // Preserve existing columns using 'first'
                        first("franchise_id").alias("franchise_id"),
                        first("franchise_name").alias("franchise_name"),
                        first("country").alias("country"),
                        first("city").alias("city"),
                        first("lat").alias("lat"),
                        first("lng").alias("lng"),
                        round(avg("avg_tmpr_c"), 2).alias("avg_tmpr_c"), // Average temperature since it is all over different for the same day and location
                        first("total_cost").alias("total_cost"),
                        first("discount").alias("discount"),
                        round(first("real_total_cost"), 2).alias("real_total_cost"),
                        count("*").alias("items_count") // Count number of receipts
                )
                .withColumn("order_size", // Put order size name based on items count
                        when(col("items_count").isNull().or(col("items_count").leq(0)), "Erroneous")
                                .when(col("items_count").equalTo(1), "Tiny")
                                .when(col("items_count").between(2, 3), "Small")
                                .when(col("items_count").between(4, 10), "Medium")
                                .otherwise("Large"));
        return dfWithOrderSize;
    }

    public Dataset<Row> calcateOrderTypesPerRestaurantDates(Dataset<Row> groupOrderSizeCalculated) {
        // Add count columns for different order sizes
        Dataset<Row> dfWithCounts = groupOrderSizeCalculated.groupBy("restaurant_franchise_id", "date")
                .agg(count(when(col("order_size").equalTo("Erroneous"), true)).alias("erroneous_orders"),
                        count(when(col("order_size").equalTo("Tiny"), true)).alias("tiny_orders"),
                        count(when(col("order_size").equalTo("Small"), true)).alias("small_orders"),
                        count(when(col("order_size").equalTo("Medium"), true)).alias("medium_orders"),
                        count(when(col("order_size").equalTo("Large"), true)).alias("large_orders"));

        // Get the most popular order type
        Column mostPopularOrderType = greatest(col("erroneous_orders"), col("tiny_orders"), col("small_orders"), col("medium_orders"), col("large_orders"));

        // Determine the most popular order type name
        Column mostPopularOrderTypeName = when(col("erroneous_orders").equalTo(mostPopularOrderType), "Erroneous")
                .when(col("tiny_orders").equalTo(mostPopularOrderType), "Tiny")
                .when(col("small_orders").equalTo(mostPopularOrderType), "Small")
                .when(col("medium_orders").equalTo(mostPopularOrderType), "Medium")
                .when(col("large_orders").equalTo(mostPopularOrderType), "Large");

        // Add the most popular order type column
        return dfWithCounts.withColumn("most_popular_order_type", mostPopularOrderTypeName);
    }


    public void run() {
        System.out.println("Started Spark ETL! Give us a sec...");

        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("DataPrep")
                .master("local[2]")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        // Step 3 - 4: Read the CSV file
        System.out.println("Reading CSV...");
        Dataset<Row> restaurants = readDatasetFromCsv(spark, RESTAURANTS_CSV_PATH);
        Dataset<Row> weather = readDatasetFromCsv(spark, WEATHER_CSV_PATH);

        // Step 3 - 4: Cleanse, filter and format initial data
        System.out.println("Formatting data...");
        Dataset<Row> formattedRestaurants = formatData(restaurants, "date_time");
        Dataset<Row> formattedWeather = formatData(weather, "wthr_date");

        // Step 5: Join two data sources on date and coordinates
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

        // Step 6: Filter the data by the average temperature
        Dataset<Row> temperatureFiltered = curatedDataset.filter(col("avg_tmpr_c").gt(0));

        // Step 7: Calculate real total cost, Group and populate 'order_size'
        Dataset<Row> realCostCalculated = temperatureFiltered.withColumn("real_total_cost", col("total_cost").minus(col("total_cost").multiply(col("discount"))));
        Dataset<Row> groupOrderSizeCalculated = calculateOrderSize(realCostCalculated);

        // Step 8: Calculate the quantity of each order type per restaurant per date and add "most_popular_order_type" for a restaurant for each day.
        Dataset<Row> popularQuantitiesCalculated = calcateOrderTypesPerRestaurantDates(groupOrderSizeCalculated);

        // Join back to previous dataset to get full set
        Dataset<Row> finalDataset = groupOrderSizeCalculated.as("g").join(popularQuantitiesCalculated.as("p"),
                col("g.restaurant_franchise_id").equalTo(col("p.restaurant_franchise_id"))
                        .and(col("g.date").equalTo(col("p.date"))),
                "inner")
                .drop(popularQuantitiesCalculated.col("restaurant_franchise_id"))
                .drop(popularQuantitiesCalculated.col("date"));

        // Print sample output
        printSampleOutput(finalDataset);

        // Step 9: Save the result to a new CSV file
        System.out.println("Writing final output CSV file...");
        finalDataset.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(OUTPUT_PATH);

        // Close the session
        spark.stop();
        System.out.println("Operation was successful. You can find the CSV files in the output folder.");
        System.out.println("Done! :)");


    }


    public static void main(String[] args) {
        new SparkETL().run();
    }


}
