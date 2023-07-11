package com.bdec.training.javasparkl2;

import java.io.Serializable;
import java.util.Objects;

public class ItemPrice implements Serializable {
    String itemId;
    Integer dateOfPrice;
    Double price;

    @Override
    public String toString() {
        return "ItemPrice{" +
                "itemId='" + itemId + '\'' +
                ", dateOfPrice=" + dateOfPrice +
                ", price=" + price +
                '}';
    }

    public String toCSV() {
        return itemId + "," + dateOfPrice + "," + price;
    }

    public static  String header() {
        return "itemId,dateOfPrice,price";
    }

    public ItemPrice(String itemId, Integer dateOfPrice, Double price) {
        this.itemId = itemId;
        this.dateOfPrice = dateOfPrice;
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemPrice itemPrice = (ItemPrice) o;
        return Objects.equals(itemId, itemPrice.itemId) &&
                Objects.equals(dateOfPrice, itemPrice.dateOfPrice) &&
                Objects.equals(price, itemPrice.price);
    }

    @Override
    public int hashCode() {

        return Objects.hash(itemId, dateOfPrice, price);
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Integer getDateOfPrice() {
        return dateOfPrice;
    }

    public void setDateOfPrice(Integer dateOfPrice) {
        this.dateOfPrice = dateOfPrice;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}