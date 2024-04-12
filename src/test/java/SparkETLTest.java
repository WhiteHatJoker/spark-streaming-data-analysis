
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.itpu.SparkStreaming;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.itpu.SparkETL;

import java.io.File;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.junit.jupiter.api.Assertions.*;

public class SparkETLTest {

    private static SparkSession spark;
    private final String TEST_OUTPUT_FOLDER = "./streamingfiles/test_output/result.csv";

    @BeforeAll
    public static void setUp() {
        spark = SparkSession.builder()
                .appName("SparkETLTest")
                .master("local[2]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        spark.stop();
    }

    @Test
    public void testReadDatasetFromCsv() {
        SparkETL sparkETL = new SparkETL();
        Dataset<Row> dataset = sparkETL.readDatasetFromCsv(spark, "src/test/resources/testRead.csv");
        assertNotNull(dataset);
        assertTrue(dataset.count() == 6, "All rows should be read");
    }

    @Test
    public void testFormatData() {
        SparkETL sparkETL = new SparkETL();
        Dataset<Row> unformattedData = sparkETL.readDatasetFromCsv(spark, "src/test/resources/testFormat.csv");
        Dataset<Row> formattedData = sparkETL.formatData(unformattedData, "date_time");
        assertNotNull(formattedData, "");
        assertEquals(3, formattedData.count(), "Only rows with date in 2022 should be left");
        assertEquals("date", formattedData.columns()[11], "The date column was added");
        assertEquals(3, formattedData.select(col("date")).distinct().count(),
                "The datetime column should be cast to date");

        assertEquals(3, formattedData.select(round(col("lat"), 2)).distinct().count(),
                "The latitude column should be rounded to 2 decimals");
        assertFalse(Arrays.toString(formattedData.columns()).contains("date_time"),
                "The date_time column should be dropped");
    }

    @Test
    public void testCalculateOrderSize() {
        SparkETL sparkETL = new SparkETL();
        Dataset<Row> uncalculatedData = sparkETL.readDatasetFromCsv(spark, "src/test/resources/testOrderSize.csv");
        Dataset<Row> calculatedData = sparkETL.calculateOrderSize(uncalculatedData);
        assertNotNull(calculatedData);
        assertEquals(3, calculatedData.count(), "Rows should be grouped by restaurant_franchise_id and receipt_id");
        assertEquals("items_count", calculatedData.columns()[calculatedData.columns().length - 2],"items_count column should be added");
        assertEquals("order_size", calculatedData.columns()[calculatedData.columns().length - 1],"order_size column should be added");
        assertEquals(3, calculatedData.select(col("order_size")).distinct().count(),
                "3 distinct types should be for order_size column");
    }

    @Test
    public void testCalcateOrderTypesPerRestaurantDates() {
        SparkETL sparkETL = new SparkETL();
        Dataset<Row> uncalculatedData = sparkETL.readDatasetFromCsv(spark, "src/test/resources/testOrderTypes.csv");
        Dataset<Row> calculatedData = sparkETL.calcateOrderTypesPerRestaurantDates(uncalculatedData);
        assertNotNull(calculatedData);
        assertEquals(1, calculatedData.count(), "Rows should be grouped by restaurant_franchise_id and date");

        assertEquals("most_popular_order_type", calculatedData.columns()[calculatedData.columns().length - 1],"most_popular_order_type column should be added");
        assertEquals(1, calculatedData.select(col("most_popular_order_type")).distinct().count(),
                "1 distinct types should be for order_size column");

        String mostPopularOrderType = calculatedData.select(col("most_popular_order_type")).head().getString(0);
        assertEquals("Tiny", mostPopularOrderType, "Incorrect value for most_popular_order_type");
    }


    @Test
    void testCreateOutputFileIfNotExists() {
        SparkStreaming sparkStreaming = new SparkStreaming();
        File outputFile = new File(TEST_OUTPUT_FOLDER);

        sparkStreaming.createOutputFileIfNotExists(TEST_OUTPUT_FOLDER);
        assertTrue(outputFile.exists());
        assertTrue(outputFile.length() > 0);
    }

    @Test
    void testGetSchema() {
        SparkStreaming sparkStreaming = new SparkStreaming();
        String expectedSchema = "receipt_id,id,franchise_id,franchise_name,country,city,lat,lng,avg_tmpr_c,total_cost,discount,real_total_cost,items_count,order_size,restaurant_franchise_id,date,erroneous_orders,tiny_orders,small_orders,medium_orders,large_orders,most_popular_order_type,";

        StructType schema = sparkStreaming.getSchema();
        assertNotNull(schema);

        // Check if the generated schema matches the expected schema
        StringBuilder actualSchema = new StringBuilder();
        for (StructField field : schema.fields()) {
            actualSchema.append(field.name()).append(",");
        }
        assertEquals(expectedSchema, actualSchema.toString());
    }
}
