package com.bdec.training.javasparkl2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class DailyDiscountCalculator {
    public static Double getTotalDiscount(Dataset<Row> itemPrice, Dataset<Row> sales) {
        Dataset<Row> discDf = itemPrice.join(sales, "itemId")
                .selectExpr("sum(totalAmount)");
        discDf.show();
        List<Row> out =  discDf.collectAsList();
        Double ret = out.get(0).getDouble(0);
        System.out.println("Discount = " + ret);
        return ret;
    }
}
