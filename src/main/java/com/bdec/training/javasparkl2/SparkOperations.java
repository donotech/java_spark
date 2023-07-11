package com.bdec.training.javasparkl2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class SparkOperations {
    static String productUrl = "file:///C:\\Training\\TVS\\dw\\product_meta.csv";
    static String salesUrl = "file:///C:\\Training\\TVS\\dw\\sales_1.csv";

    public static void sparkJoin(SparkSession spark) {
        Dataset<Row> productDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(productUrl);

        //productDf.show();

        Dataset<Row> salesDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(salesUrl);

//        salesDf.show();

//        Dataset<Row> joinedDf = salesDf.join(productDf,
//                salesDf.col("item_id").equalTo(productDf.col("item_id")),
//                "outer");

//        Dataset<Row> joinedDf = salesDf.join(productDf,
//                salesDf.col("item_id").equalTo(productDf.col("item_id")),
//                "left").filter("product_name is not null").
//                select(salesDf.col("item_id"), salesDf.col("item_qty"),
//                        salesDf.col("total_amount"));

        Dataset<Row> joinedDf = salesDf.join(productDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "semi");

        Dataset<Row> groupedDf = joinedDf.groupBy( "date_of_sale")
                .agg(functions.sum("total_amount").as("TotalAmount"));

        //groupedDf.show();

        joinedDf.sqlContext().udf().register("WeekIdFromMonth",
                (UDF1<String, Integer>) dateddmmyyyy -> {
            String day = dateddmmyyyy.substring(0,2);
            Integer dayInt = Integer.parseInt(day);
            Integer weekId = 0;
            if(dayInt <= 6) {
                weekId = 1;
            } else if(dayInt <= 13) {
                weekId = 2;
            } else if(dayInt <= 21) {
                weekId = 3;
            } else if(dayInt <= 28) {
                weekId = 4;
            } else {
                weekId = 5;
            }

            return weekId;
        }, DataTypes.IntegerType);


        Dataset<Row> dsWithWeekId = joinedDf.selectExpr("*",
                "WeekIdFromMonth(date_of_sale) as WeekId");
        dsWithWeekId.show();

    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        sparkJoin(spark);

    }
}
