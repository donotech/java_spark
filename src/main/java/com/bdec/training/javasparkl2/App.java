package com.bdec.training.javasparkl2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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