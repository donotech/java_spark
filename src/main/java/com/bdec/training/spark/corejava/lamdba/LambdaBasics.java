package com.bdec.training.spark.corejava.lamdba;


interface ClickEventHandler {
    void handleClick(int digit);
}

class Button {
    int digit;
    public Button(int digit) { this.digit = digit; }
    public void ButtonPressed(ClickEventHandler h) { h.handleClick(digit); }
}

public class LambdaBasics {

    public static void usingAnonymous() {
        Button b1 = new Button(1);
        b1.ButtonPressed(new ClickEventHandler() {
            @Override
            public void handleClick(int digit) {
                System.out.println("Printing on display board " + digit);
            }
        });
    }

    public static void usingLambda() {
        Button b1 = new Button(1);
        b1.ButtonPressed(digit -> System.out.println("Printing on LED board " + digit));
    }

    public static void usingLambdaWithLocal() {
        String prefix = "Prefix";
        Button b1 = new Button(1);
        b1.ButtonPressed(digit -> System.out.println("Printing on LED board " + prefix + " " + digit));
    }

    public static void main(String[] args) {
        usingAnonymous();
        usingLambda();
        usingLambdaWithLocal();
    }
}
