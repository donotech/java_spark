package com.bdec.training.spark.corejava.lamdba;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.*;

class AddThree implements Function<Long, Long> {

    @Override
    public Long apply(Long aLong) {
        return aLong + 3;
    }
}

public class FunctionalDemo {
    public static void main(String[] args) {
        List<Integer> numbers = new ArrayList<>();
        Collections.sort(numbers, (n1, n2) -> n1.compareTo(n2));

        //function old style
        Function<Long, Long> adder = new AddThree();
        Long result = adder.apply((long) 4);
        System.out.println("result = " + result);

        //annonymous class style
        Function<Long, Long> adderOldStyle = new Function<Long, Long>() {
            @Override
            public Long apply(Long aLong) {
                return aLong + 3;
            }
        };

        //function lambda style
        Function<Long, Long> adderNew = (value) -> value + 3;
        Function<Long, Long> adderNew2 = (value) -> { return value + 3; };


        Integer ADDER_CONST = 3;
        Function<Long, Long> adderNewExternal = (value) -> value + ADDER_CONST;
        adderNewExternal.apply(5L);
//        ADDER_CONST = 33;
//        adderNewExternal.apply(5L);

        //BiFunction<>

        Long resultLambda = adderNew.apply((long) 8);
        System.out.println("resultLambda = " + resultLambda);

        Predicate predicate = (value) -> value != null;
        Predicate predicate2 = (value) -> { if(value != null) return true; else return false;};

        Function<String, Integer> f = String::length;

        int len = f.apply("Hello World");

        //rude rules
        Predicate<String> ruleOfExlaim = (String value) -> value.contains("!!!");
        Predicate<String> ruleOfStars = (value) -> value.contains("***");
        Predicate<String> ruleOfCaps = (value) -> value.startsWith("X");

        Predicate<String> rudeChecker = ruleOfExlaim.or(ruleOfStars).or(ruleOfCaps);
        Boolean isRude = rudeChecker.test("Some String");


        Supplier<Double> supplier = () -> (Math.random());
        Consumer<Integer> consumer = (value) -> System.out.println(value);
        BinaryOperator<Integer> binaryOperator =
                (value1, value2) -> { return value1 + value1; };
        UnaryOperator<String> unaryOperator =
                (p) -> { return p + " applied operator"; };
    }


}
