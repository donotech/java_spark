package com.bdec.training.spark.corejava.java_threading;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

class SampleClass {
    public SampleClass() {
        System.out.println("Sample Class object initialized");
    }

    public void hello() {
        System.out.println("hello from sample");
    }
}

public class ClassLoaderDemo {

    public static String out() {
        String s = new String();
        String s2 = s;
        return s;
    }

    public String something() {
        String ref = out();
        return ref;
    }

    public static void main(String[] args) {
        try
        {
            Class<?> cls = Class.forName("java_threading.SampleClass");

            System.out.println("Class Name: " + cls.getName());
            System.out.println("Package Name: " + cls.getPackage());
            Method[] methods = cls.getDeclaredMethods();
            System.out.println("-----Methods of String class -------------");
            for (Method method : methods)
            {
                System.out.println(method.getName());

            }

            Constructor<?> ctor = cls.getConstructor();
            Object object = ctor.newInstance();
            System.out.println("Invoking the first method");
            methods[0].invoke(object);

        }
        catch (ClassNotFoundException |
                NoSuchMethodException |
                IllegalAccessException |
                InstantiationException |
                InvocationTargetException e)
        {
            e.printStackTrace();
        }

        List<String> l = new ArrayList<>();
        for(int i =0; i < 10; i++) {
            l.add(new String(i + ""));
        }
        System.gc();
        //program stsrt
        ///......
//        f1();
//        f2();
//        f3();
    }
}
