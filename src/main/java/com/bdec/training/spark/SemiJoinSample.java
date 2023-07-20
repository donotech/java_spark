package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import static com.bdec.training.spark.SparkOperations.*;

public class SemiJoinSample {
    public static void main(String[] args) {
        String winUtilPath = "C:\\softwares\\winutils\\bin";
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

        Dataset<Row> salesDf = readSales(spark, sales2Url);
        Dataset<Row> productDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(productUrl);

        salesDf.createOrReplaceTempView("sales");
        productDf.createOrReplaceTempView("product");

        Dataset<Row> joinedDf = spark.sql("select s.* " +
                "from sales s join product p " +
                "on s.item_id = p.item_id");

        Dataset<Row> semiJoinDf = spark.sql("select * " +
                "from sales s left semi join product p " +
                "on s.item_id = p.item_id");
        semiJoinDf.show();
    }

}
