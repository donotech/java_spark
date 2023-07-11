package com.bdec.training.testing;

import com.bdec.training.javasparkl2.DailyDiscountCalculator;
import com.bdec.training.javasparkl2.ItemPrice;
import com.bdec.training.javasparkl2.Sales;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.SparkSession;

public class DiscountTest {
    private static SparkSession spark;

    @BeforeClass
    public static void setUp() {
        String winutilPath = "C:\\devraj\\winutils"; //\\bin\\winutils.exe"; //bin\\winutils.exe";

        if(System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("Detected windows");
            System.setProperty("hadoop.home.dir", winutilPath);
            System.setProperty("HADOOP_HOME", winutilPath);
        }


        spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();


    }

    @AfterClass
    public static void tearDown() {
        spark.stop();
    }

    @Test
    public void testDiscount() {
        List<ItemPrice> itemList = Arrays.asList(
                new ItemPrice("1", 20200101, 10.0),
                new ItemPrice("2", 20200101, 100.0)
        );

        List<Sales> salesList = Arrays.asList(
                new Sales("1", "1", 100.0, 900.0, 20200101), //
                new Sales("1", "2", 1.0, 90.0, 20200101)
                );

        Dataset<Row> itemDf = spark.createDataset(itemList, Encoders.bean(ItemPrice.class)).toDF();
        Dataset<Row> salesDf = spark.createDataset(salesList, Encoders.bean(Sales.class)).toDF();

        Dataset<Row> joinedDf = itemDf.join(salesDf, "itemId");
        joinedDf.show();
        double totalDiscount = DailyDiscountCalculator.getTotalDiscount(itemDf, salesDf);
        Assert.assertEquals(totalDiscount, 990.0, 0.01);

        //joinedDf.rdd().getNumPartitions();
        //joinedDf.repartition(10, joinedDf.col("itemId"));

//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        //now write some asserts here

    }

    @Test
    public void testSalesChange() {

        List<Sales> salesListJan = Arrays.asList(
                new Sales("1", "1", 100.0, 900.0, 20200101),
                new Sales("1", "1", 50.0, 400.0, 20200102), //
                new Sales("1", "2", 1.0, 90.0, 20200101)
        );

        List<Sales> salesListFeb = Arrays.asList(
                new Sales("1", "1", 150.0, 1500.0, 20200102), //
                new Sales("1", "2", 1.0, 85.0, 20200102)
        );

        Dataset<Row> janDf = spark.createDataset(salesListJan, Encoders.bean(Sales.class)).toDF();
        Dataset<Row> febDf = spark.createDataset(salesListFeb, Encoders.bean(Sales.class)).toDF();

        Dataset<Row> joinedDf = janDf.union(febDf);

        joinedDf.collectAsList().forEach(System.out::println);
        //now write some asserts here

    }

    void setupHiveTables() {
        List<ItemPrice> itemList = Arrays.asList(
                new ItemPrice("1", 20200101, 10.0),
                new ItemPrice("2", 20200101, 100.0)
        );

        List<Sales> salesList = Arrays.asList(
                new Sales("1", "1", 100.0, 900.0, 20200101), //
                new Sales("1", "2", 1.0, 90.0, 20200101)
        );

        Dataset<Row> itemDf = spark.createDataset(itemList, Encoders.bean(ItemPrice.class)).toDF();
        Dataset<Row> salesDf = spark.createDataset(salesList, Encoders.bean(Sales.class)).toDF();

        spark.sql("create database if not exists firstdb");
        itemDf.write().saveAsTable("firstdb.item_t");
        salesDf.write().saveAsTable("firstdb.sales_t");

    }

    @Test
    public void testDiscountWithHive() {
        setupHiveTables();
//        Dataset<Row> itemTable = spark.table("firstdb.item_t");
//        Dataset<Row> salesTable = spark.table("firstdb.sales_t");

        Dataset<Row> joinedDf = spark.sql("select * from firstdb.item_t i join firstdb.sales_t s on i.itemId = s.itemId");

        joinedDf.show();
        //now write some asserts here
    }
}
