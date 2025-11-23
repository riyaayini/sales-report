package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GlobalSuperstoreReport {

    public static void main(String[] args) {

        // 🔧 FIX for Windows Hadoop Requirement
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        // 🛑 Validate Input Arguments
        if (args.length < 3) {
            System.out.println("Usage: <sales_input_path> <returns_input_path> <output_path>");
            System.exit(1);
        }

        String salesPath = args[0];
        String returnsPath = args[1];
        String outputPath = args[2];

        // 🚀 Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Global Superstore Report")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // ⚙️ Fix Date Parsing Issue on Spark 3.x
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        // 📥 Load CSV Files
        Dataset<Row> salesDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(salesPath);

        Dataset<Row> returnsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(returnsPath);

        // ❌ Remove Returned Orders
        Dataset<Row> validSalesDF = salesDF.join(
                returnsDF,
                salesDF.col("Order ID").equalTo(returnsDF.col("Order ID")),
                "leftanti"
        );

        // 📅 Parse Date, Derive Year & Month
        Dataset<Row> enrichedSales = validSalesDF
                .withColumn("Order_Date_Parsed", to_date(col("Order Date"), "M/d/yyyy"))
                .withColumn("Year", year(col("Order_Date_Parsed")))
                .withColumn("Month", month(col("Order_Date_Parsed")));

        // 📊 Generate Report
        Dataset<Row> result = enrichedSales.groupBy("Year", "Month", "Category", "Sub-Category")
                .agg(
                        sum("Quantity").alias("Total_Quantity_Sold"),
                        round(sum("Profit"), 2).alias("Total_Profit")
                )
                .orderBy("Year", "Month", "Category", "Sub-Category");

        System.out.println("📌 Sample Output:");
        result.show(20, false);

        // 💾 Save As Partitioned Parquet
        result.write()
                .mode("overwrite")
                .partitionBy("Year", "Month")
                .parquet(outputPath);

        System.out.println("✔ Report Generated Successfully → " + outputPath);

        // 🛑 Close Spark
        spark.stop();
    }
}