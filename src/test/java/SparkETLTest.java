
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.itpu.SparkETL;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.junit.jupiter.api.Assertions.*;

public class SparkETLTest {

    private static SparkSession spark;

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
        assertTrue(formattedData.columns()[11].equals("date"), "The date column was added");
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
        Dataset<Row> uncalculatedData = sparkETL.readDatasetFromCsv(spark, "src/test/resources/test.csv");
        Dataset<Row> calculatedData = sparkETL.calculateOrderSize(uncalculatedData);
        assertNotNull(calculatedData);
        assertTrue(calculatedData.columns().length > 0);
        assertTrue(calculatedData.columns()[calculatedData.columns().length - 1].equals("order_size"));
    }

    @Test
    public void testCalcateOrderTypesPerRestaurantDates() {
        SparkETL sparkETL = new SparkETL();
        Dataset<Row> uncalculatedData = sparkETL.readDatasetFromCsv(spark, "src/test/resources/test.csv");
        Dataset<Row> calculatedData = sparkETL.calcateOrderTypesPerRestaurantDates(uncalculatedData);
        assertNotNull(calculatedData);
        assertTrue(calculatedData.columns().length > 0);
        assertTrue(calculatedData.columns()[calculatedData.columns().length - 1].equals("most_popular_order_type"));
    }
}
