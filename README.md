# Restaurant Orders ETL and Streaming with Apache Spark

A two-stage data pipeline in Java and Apache Spark. A batch ETL job joins restaurant receipts with daily weather, calculates real order costs, classifies orders by size, and finds each restaurant's most popular order type per day. A Spark Streaming job then watches a folder for new results, flags days warm enough for a cold-drinks promotion, and appends them to a running output file.

![Java](https://img.shields.io/badge/Java-17-orange) ![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1%20(SQL%20%2B%20Streaming)-e25a1c) ![JUnit](https://img.shields.io/badge/Tests-JUnit%205-25a162)

## Pipeline

```
restaurant_csv/ (receipts) ──┐
                             ├─► SparkETL ──► output/*.csv ──copy──► streamingfiles/input/
weather_csv/ (daily temps) ──┘                                              │
                                                                            ▼
                                             SparkStreaming (polls every 5 s) ──► streamingfiles/output/result.csv
```

### Stage 1: batch ETL (`SparkETL`)

1. **Read** receipts and weather from CSV.
2. **Format** both sources: keep only 2022 records, convert timestamps to dates, round coordinates to 2 decimals so the two sources line up.
3. **Join** receipts to weather on date, latitude and longitude (inner join), then drop the duplicate columns.
4. **Filter** to days with an average temperature above 0 °C.
5. **Calculate** the real total cost after discount: `total_cost − total_cost × discount`.
6. **Classify** each order by item count:

   | Items | Order size |
   |-------|------------|
   | 0 or missing | Erroneous |
   | 1 | Tiny |
   | 2–3 | Small |
   | 4–10 | Medium |
   | 11+ | Large |

7. **Aggregate** per restaurant per day: the count of each order size, plus `most_popular_order_type`.
8. **Write** the result as CSV to `output/`.

### Stage 2: streaming (`SparkStreaming`)

- A `JavaStreamingContext` with a 5-second batch interval watches `streamingfiles/input/` for new files.
- Each new batch is parsed, `avg_tmpr_c` is cast to a number, and a `promo_cold_drinks` column is added: `true` when the day's average temperature is above 25 °C.
- Results are appended to `streamingfiles/output/result.csv`, which is created with a header on first run.

## Run it

Requirements: JDK 17 and Maven.

Spark 3.5 on Java 17 needs one extra JVM flag:

```
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
```

The IntelliJ IDEA run configurations in `.run/` already include it.

![Run configuration](./screenshots/config1.png)

**1. Run the ETL job**

Run `org.itpu.SparkETL` (or, from the command line):

```bash
MAVEN_OPTS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED" \
  mvn compile exec:java -Dexec.mainClass=org.itpu.SparkETL
```

CSV files appear in `output/`.

![ETL output](./screenshots/DataPrep.png)

**2. Start the streaming job**

Run `org.itpu.SparkStreaming`. It prints "For now the input stream is empty" every 5 seconds while it waits.

**3. Feed it data**

Copy a CSV file from `output/` into `streamingfiles/input/`. Spark Streaming only picks up files that are newly created, so copy the file rather than moving an old one, or `touch` it to update its timestamp. Within a few seconds the rows are processed and appended to `streamingfiles/output/result.csv`.

![Streaming output](./screenshots/SparkStreaming.png)

## Tests

`SparkETLTest` (JUnit 5, local Spark session) covers CSV reading, formatting, order-size classification, per-restaurant aggregation, output file creation and the streaming schema.

```bash
mvn test -DargLine="--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
```

![Test results](./screenshots/SparkTest.png)

## Project structure

```
src/main/java/org/itpu/SparkETL.java        Batch stage: join, cost, order size, aggregation
src/main/java/org/itpu/SparkStreaming.java  Streaming stage: folder watch, promo flag, append
src/test/java/SparkETLTest.java             JUnit 5 tests
src/main/resources/                         Input receipts and weather CSVs
output/                                     Sample ETL output
streamingfiles/input/, streamingfiles/output/  Sample streaming input and result
.run/                                       IntelliJ run configs with the JVM flag
```

---

Built as a data engineering assignment at ITPU. Author: Ravshan Khudoyberdiev.
