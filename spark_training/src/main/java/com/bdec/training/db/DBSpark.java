package com.bdec.training.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DBSpark {
    public static void main(String[] args) {
        String sourcePath = args[0];
        String targetTable = args[1];
        SparkSession spark = SparkSession.builder().appName("DBTraining").getOrCreate();

        Dataset<Row> dfSrc = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(sourcePath);

        dfSrc.show();
        Dataset<Row> dfN = dfSrc.select("neighbourhood_group").distinct();
        dfN.write().mode(SaveMode.Overwrite).saveAsTable(targetTable);
    }
}
