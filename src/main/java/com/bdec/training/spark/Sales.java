package com.bdec.training.spark;

import java.io.Serializable;
import java.util.Objects;

public class Sales implements Serializable {
    String storeId;
    String itemId;
    Double qty;
    Double totalAmount;
    Integer dateOfSale;

    @Override
    public String toString() {
        return "Sales{" +
                "storeId='" + storeId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", qty=" + qty +
                ", totalAmount=" + totalAmount +
                ", dateOfSale=" + dateOfSale +
                '}';
    }

    public Sales() {}
    public String toCSV() {
        return storeId + "," + itemId + "," + qty + "," + totalAmount + "," + dateOfSale;
    }

    public static String header() {
        return "storeId,itemId,qty,totalAmount,dateOfSale";
    }

    public Sales(String storeId, String itemId, Double qty, Double totalAmount, Integer dateOfSale) {
        this.storeId = storeId;
        this.itemId = itemId;
        this.qty = qty;
        this.totalAmount = totalAmount;
        this.dateOfSale = dateOfSale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sales sales = (Sales) o;
        return Objects.equals(storeId, sales.storeId) &&
                Objects.equals(itemId, sales.itemId) &&
                Objects.equals(qty, sales.qty) &&
                Objects.equals(totalAmount, sales.totalAmount) &&
                Objects.equals(dateOfSale, sales.dateOfSale);
    }

    @Override
    public int hashCode() {

        return Objects.hash(storeId, itemId, qty, totalAmount, dateOfSale);
    }

    public String getStoreId() {
        return storeId;
    }

    public void setStoreId(String storeId) {
        this.storeId = storeId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Double getQty() {
        return qty;
    }

    public void setQty(Double qty) {
        this.qty = qty;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Integer getDateOfSale() {
        return dateOfSale;
    }

    public void setDateOfSale(Integer dateOfSale) {
        this.dateOfSale = dateOfSale;
    }
}
