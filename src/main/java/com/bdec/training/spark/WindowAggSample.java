package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import static com.bdec.training.spark.SparkOperations.readSales;
import static com.bdec.training.spark.SparkOperations.sales2Url;

public class WindowAggSample {
    /*
     *  sales data weekly, percentage sales happened in the 1st week, 2nd week, 3rd week, 4th week
     */

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

        Dataset<Row> sales2Df = readSales(spark, sales2Url);
        sales2Df.createOrReplaceTempView("sales");
        Dataset<Row> groupedDf = spark.sql("select item_id, sum(total_amount) as item_total" +
                " from sales group by item_id");
        groupedDf.createOrReplaceTempView("itemtotal");
        Dataset<Row> groupJoinedDf = spark.sql("select s.*, i.item_total " +
                "from sales s join itemtotal i on s.item_id = i.item_id");
        //groupJoinedDf.show();
        WindowSpec windowSpec = Window.partitionBy("item_id");

        Dataset<Row> windowedDf = sales2Df.withColumn("item_total",
                functions.sum("total_amount").over(windowSpec));

        //windowedDf.show();

        WindowSpec rankSpec = Window.partitionBy("date_of_sale")
                .orderBy("total_amount");
        Dataset<Row> rankWindowDf = sales2Df.withColumn("date_wise_rank",
                functions.rank().over(rankSpec)).where("date_wise_rank");

        rankWindowDf.show();

//
//        sales2Df.show();
//        groupedDf.show();

    }

    public static void semiJoin() {
        /*
         * item totals which are part of my category
         */
    }
}
