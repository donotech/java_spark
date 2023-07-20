package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class AQEDemo {
    public static void main(String[] args) {
        String winUtilPath = "C:\\devraj\\winutils";
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

        String airBnbData = "file:///C:\\devraj\\training\\java_spark\\airbnb\\NYC-Airbnb-2023.csv";
        Dataset<Row> airBnbDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(airBnbData);

        Dataset<Row> locationGroupedDf = airBnbDf.groupBy("neighbourhood_group").count();

        spark.conf().set("spark.sql.adaptive.enabled",false);
//        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled",true);
        //hint
//        spark.sql("select * from prices join industry /*broadcast */ " +
//                " on prices.ticker = industry.ticker");
//        airBnbDf.join(airBnbDf).hint("sort-merge join");


        System.out.println(locationGroupedDf.rdd().getNumPartitions());


    }
}
