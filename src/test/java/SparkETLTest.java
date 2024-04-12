
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.itpu.SparkETL;

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
        assertTrue(dataset.count() == 6);
    }

    @Test
    public void testFormatData() {
        SparkETL sparkETL = new SparkETL();
        Dataset<Row> unformattedData = sparkETL.readDatasetFromCsv(spark, "src/test/resources/test.csv");
        Dataset<Row> formattedData = sparkETL.formatData(unformattedData, "date_time");
        assertNotNull(formattedData);
        assertEquals(3, formattedData.columns().length);
        assertTrue(formattedData.columns()[0].equals("date"));
        assertTrue(formattedData.columns()[1].equals("lat"));
        assertTrue(formattedData.columns()[2].equals("lng"));
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
