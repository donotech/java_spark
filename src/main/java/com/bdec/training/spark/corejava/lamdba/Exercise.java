package com.bdec.training.spark.corejava.lamdba;

import java.util.ArrayList;
import java.util.List;

class Product {
    int productId;
    int productPrice;

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public int getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
    }
}

class Bill {
    Integer billDate; //in form 20220825
    Integer customerId;
    List<String> itemsSold;

    public Integer getBillDate() {
        return billDate;
    }

    public void setBillDate(Integer billDate) {
        this.billDate = billDate;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public List<String> getItemsSold() {
        return itemsSold;
    }

    public void setItemsSold(List<String> itemsSold) {
        this.itemsSold = itemsSold;
    }
}

public class Exercise {

    public static void billCopies() {
        List<Bill> bills = new ArrayList<>();
        //to print the total sales for the month of august 2022 i.e. from 20220201 to 20220831
    }

    public static void main(String[] args) {
        List<Integer> salaries = new ArrayList<>();
        salaries.add(1000);
        salaries.add(2000);
        salaries.add(5000);
        salaries.add(10000);

        //tax slabs
        // salary as < 2000 then tax is 0
        // salary <= 5000 > 2000 then tax is 10% of salary
        // salary > 5000 then tax is 30% of salary

        //you have to print the total tax liability of a company - salaries array

    }
}
