package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.bdec.training.spark.SparkOperations.productUrl;

public class SparkReadFilesSample {
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
        String writeURL = "C:\\tmp\\spark_output2";

        Dataset<Row> productDf = spark.read()
//                .option("inferSchema", "true")
//                .option("header", "true")
                .csv(writeURL);
        productDf.filter("_c0 = 2").explain();

       // System.out.println(productDf.count());

    }
}
