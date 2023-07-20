package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static com.bdec.training.spark.SparkOperations.*;

public class DataPartitioningSample {
    public static void main(String[] args) {
        String winUtilPath = "C:\\softwares\\winutils";
        if(System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("detected windows");
            System.setProperty("hadoop.home.dir", winUtilPath);
            System.setProperty("HADOOP_HOME", winUtilPath);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> productDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(productUrl);

        //productDf.show();

        Dataset<Row> salesDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(salesUrl);

        //salesDf.show();

        Dataset<Row> sales2Df = readSales(spark, sales2Url);
        //sales2Df.show();
        //sales2Df.printSchema();
        Dataset<Row> castedDf = salesDf
                .withColumn("date_casted", salesDf.col("date_of_sale").cast(DataTypes.DateType))
                .withColumn("unit_price_casted", salesDf.col("date_of_sale").cast(DataTypes.FloatType));
        salesDf = castedDf
                .drop("date_of_sale").withColumnRenamed("date_casted", "date_of_sale")
                .drop("unit_price").withColumnRenamed("unit_price_casted", "unit_price");
        //salesDf.printSchema();

        Dataset<Row> allSalesDf = salesDf.select("item_id", "item_qty", "unit_price", "total_amount", "date_of_sale")
                .union(sales2Df.select("item_id", "item_qty", "unit_price", "total_amount", "date_of_sale"));
        Dataset<Row> joinedDf = salesDf.join(productDf,
                salesDf.col("item_id")
                        .equalTo(productDf.col("item_id")),
                "inner").drop(productDf.col("item_id"));

        String writeURL = "C:\\tmp\\spark_output";

        //.repartition(10) to increase the number of partitions
        joinedDf.coalesce(10).write().mode(SaveMode.Overwrite)
                .partitionBy("date_of_sale").csv(writeURL);

        /*
        product 200 partitions - files
        sales 200 partitions - files
        joined - 40,000 files - will be created

        df = spark.read.df() reading joined and doing some analysis
        df.max()
        df operation will create 40K tasks
        each of these tasks will be dealing with a few records
        another operation
        df.sum()
        again same 40K tasks

        - to resolve the problem of too many small part files being created you can use
        -  repartition or coalesce
         */

        /*
        productDf = spark.read... - Product File
        productDf.cache()
        salesDf = spark.read. - Sales File
        joinedDf = sales joined productDf
        joinedDf.write
        customerDf = spark.read. - Customer File
        custJoined = sales joined customer
        custJoined.write
        allJoinedDf = joinedDf join customerDf
        allJoinedDf.write
         */
    }
}
