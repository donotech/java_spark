package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Hello world!
 */
public class App {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String INPUT_PATH = "C:\\devraj\\neptune.txt";

    public static void main(String[] args) throws IOException {
        //System.setProperty("HADOOP_HOME", "C:\\devraj\\winutils");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().text(INPUT_PATH);
        df.show();
        spark.stop();
    }
}