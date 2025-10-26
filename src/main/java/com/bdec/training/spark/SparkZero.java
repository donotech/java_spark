package com.bdec.training.spark;

import org.apache.spark.sql.SparkSession;

public class SparkZero {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        spark.stop();
    }
}
