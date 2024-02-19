package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.Locale;

public class SparkOperations {
    static String productUrl = "file:///C:\\Training\\TVS\\dw\\product_meta.csv";
    static String salesUrl = "file:///C:\\Training\\TVS\\dw\\sales_1.csv";
    static String sales2Url = "file:///C:\\Training\\TVS\\dw\\sales_2.csv";

    public static Dataset<Row> readSales(SparkSession spark, String filePath) {
        //
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("item_id",  DataTypes.IntegerType, true),
                DataTypes.createStructField("item_qty", DataTypes.IntegerType, true),
                DataTypes.createStructField("unit_price", DataTypes.FloatType, true),
                DataTypes.createStructField("total_amount", DataTypes.IntegerType, true),
                DataTypes.createStructField("date_of_sale", DataTypes.DateType, true)
        });

        Dataset<Row> productDf = spark.read()
                .schema(schema)
                .option("header", "true")
                .csv(filePath);

        return productDf;
    }

    public static void sparkOperations(SparkSession spark) {
        Dataset<Row> productDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(productUrl);

        //productDf.show();

        Dataset<Row> salesDf = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(salesUrl);

        //salesDf.show();

        Dataset<Row> sales2Df = readSales(spark, sales2Url);
        //sales2Df.show();
        //sales2Df.printSchema();
        Dataset<Row> castedDf = salesDf
                .withColumn("date_casted", salesDf.col("date_of_sale").cast(DataTypes.DateType))
                .withColumn("unit_price_casted", salesDf.col("date_of_sale").cast(DataTypes.FloatType));
        salesDf = castedDf
                .drop("date_of_sale").withColumnRenamed("date_casted", "date_of_sale")
                .drop("unit_price").withColumnRenamed("unit_price_casted", "unit_price");
        //salesDf.printSchema();

        Dataset<Row> allSalesDf = salesDf.select("item_id", "item_qty", "unit_price", "total_amount", "date_of_sale")
                .union(sales2Df.select("item_id", "item_qty", "unit_price", "total_amount", "date_of_sale"));
        allSalesDf.show();
        Dataset<Row> joinedDf = productDf.join(salesDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "left_outer").where(salesDf.col("item_id").isNotNull());
        joinedDf.show();

        Dataset<Row> semiJoinedDf = productDf.join(salesDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "left_semi");
        semiJoinedDf.show();

        //with SQL
        salesDf.createOrReplaceTempView("sales");
        productDf.createOrReplaceTempView("product");

        Dataset<Row> temp = spark.sql("select * from sales a join product b " +
                "on a.item_id = b.item_id");
        //temp.explain();
        joinedDf.explain();

        Dataset<Row> missingProductsDf = productDf.join(salesDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "left_outer").filter(salesDf.col("item_id").isNull());
        Dataset<Row> missingProductsDf2 = spark.sql(
                "select product.* from product left outer join sales " +
                        "on product.item_id = sales.item_id where sales.item_id is null");

        missingProductsDf2 = spark.sql(
                "select * from product left anti join sales " +
                        "on product.item_id = sales.item_id");

        Dataset<Row> joinedDf2 = salesDf.join(productDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "left").filter("product_name is not null").
                select(salesDf.col("item_id"), salesDf.col("item_qty"),
                        salesDf.col("total_amount"));

        Dataset<Row> joinedDf3 = salesDf.join(productDf,
                salesDf.col("item_id").equalTo(productDf.col("item_id")),
                "left_outer"); //.hint("skew", "item_id");
        joinedDf3.explain();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//
//        Dataset<Row> groupedDf = joinedDf.groupBy( "date_of_sale")
//                .agg(functions.sum("total_amount").as("TotalAmount"));
////
//        groupedDf.show();
//
//
//        spark.sqlContext().udf().register("WeekIdFromMonth",
//                (UDF1<String, Integer>) dateddmmyyyy -> {
//            String day = dateddmmyyyy.substring(0,2);
//            Integer dayInt = Integer.parseInt(day);
//            Integer weekId = 0;
//            if(dayInt <= 6) {
//                weekId = 1;
//            } else if(dayInt <= 13) {
//                weekId = 2;
//            } else if(dayInt <= 21) {
//                weekId = 3;
//            } else if(dayInt <= 28) {
//                weekId = 4;
//            } else {
//                weekId = 5;
//            }
//
//            return weekId;
//        }, DataTypes.IntegerType);
//
//        Dataset<Row> dsWithWeekId = joinedDf.selectExpr("*",
//                "WeekIdFromMonth(cast(date_of_sale as string)) as WeekId");
//
//        dsWithWeekId.show();

    }

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

        sparkOperations(spark);

    }
}
