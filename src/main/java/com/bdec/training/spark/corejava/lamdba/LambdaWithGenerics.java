package com.bdec.training.spark.corejava.lamdba;

interface DisplayBoard<T> {
    void print(T t);
}

class NewButton {
    int digit;
    public NewButton(int digit) { this.digit = digit; }
    public void onClick(DisplayBoard b) { b.print(digit); }
}

public class LambdaWithGenerics {
    public static void main(String[] args) {
        NewButton b2 = new NewButton(2);
        b2.onClick(new DisplayBoard<Integer>() {
            @Override
            public void print(Integer o) {
                System.out.println(o);
            }
        });

        NewButton b3 = new NewButton(3);
        b3.onClick((DisplayBoard<Integer>) o -> System.out.println(o));
    }
}
