package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class AQEDemo {
    public static void main(String[] args) {
        String winUtilPath = "C:\\softwares\\winutils";
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("detected windows");
            System.setProperty("hadoop.home.dir", winUtilPath);
            System.setProperty("HADOOP_HOME", winUtilPath);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        String airBnbData = "file:///C:\\Training\\airbnb\\NYC-Airbnb-2023.csv";
        Dataset<Row> airBnbDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(airBnbData);

        Dataset<Row> locationGroupedDf = airBnbDf.groupBy("neighbourhood_group").count();

        spark.conf().set("spark.sql.adaptive.enabled",true);
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled",true);


        System.out.println(locationGroupedDf.rdd().getNumPartitions());


    }
}
