package com.bdec.training.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;

public class DailyDiscountCalculator {
    public static Double getTotalDiscount(Dataset<Row> itemPrice, Dataset<Row> sales) {
        Dataset<Row> discDf = itemPrice.join(sales, "itemId")
                //.agg(functions.sum("totalAmount")).alias("sum_total_amount")
                .selectExpr("sum(totalAmount)");
        discDf.show();
        List<Row> out =  discDf.collectAsList();
        Double ret = out.get(0).getDouble(0);
        System.out.println("Discount = " + ret);
        return ret;
    }

    public static Dataset<Row> getStores(Dataset<Row> sales) {
        return sales.selectExpr("distinct(storeId)");
    }
}
