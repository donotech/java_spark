package com.bdec.training.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;


public class DataFrameSimpleOps {
    static String url = "file:///C:\\Training\\sockgen_spark_java\\salary.csv";

    public static void dataframeVersion(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(url);

//        df.show();
//        df.printSchema();
//        df.selectExpr("name", "age", "(salary/80) as dollar_salary").show();
        Dataset<Row> dfGBP = df.withColumn("GBP_Salary", df.col("salary").divide(80))
                .drop("age")
                .withColumnRenamed("designation", "company_designation")
                .dropDuplicates();

        dfGBP.explain();
        //df.filter("age > 25").select("name", "designation").show();
    }

    public static void datasetVersion(SparkSession spark) {

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(url);
//        df.show();
//        df.printSchema();
//        df.filter("age > 25").show();

        Dataset<Person> x = df.as(Encoders.bean(Person.class));
        Dataset<Person> filteredDs = x.filter((FilterFunction<Person>)
                value -> value.getAge() < 25);
        filteredDs.show();

        Dataset<Person> adult = x.filter((FilterFunction<Person>) p -> p.isAdult());
        adult.show();

    }

    public static void sqlVersion(SparkSession spark) throws AnalysisException {

        Dataset<Row> df = spark.read()
                .option("inferSchema", "true")
                .option("header", "true").csv(url);
        df.createOrReplaceTempView("emptable");
        spark.sql("select * from emptable where age > 25").show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        dataframeVersion(spark);
//        datasetVersion(spark);
//        try {
//           sqlVersion(spark);
//        } catch (AnalysisException e) {
//            e.printStackTrace();
//        }
    }
}
