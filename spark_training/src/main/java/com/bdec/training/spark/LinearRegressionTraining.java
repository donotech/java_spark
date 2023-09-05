package com.bdec.training.spark;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class LinearRegressionTraining {
    static String housepriceUrl = "file:///C:\\Training\\ml_dataset\\Melbourne_house_price.csv";

    public static void main(String[] args) {
        String winUtilPath = "C:\\softwares\\winutils";
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("detected windows");
            System.setProperty("hadoop.home.dir", winUtilPath);
            System.setProperty("HADOOP_HOME", winUtilPath);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SocgenJava")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> rawDf = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(housepriceUrl);

        Dataset<Row> requiredFields = rawDf.filter("Suburb='Abbotsford'").drop("Suburb", "Address", "Method", "Type",
                "SellerG", "Date", "Postcode", "CouncilArea", "Lattitude",
                "Distance", "PropertyCount",
                "Longtitude", "RegionName");

        Dataset<Row> fieldsForTraining = requiredFields.na().drop().selectExpr(
                "cast(Rooms as double)", "cast(Price as double)",
                "cast(Bedroom2 as double)", "cast(Bathroom as double)", "cast(Car as double)",
                "cast(Landsize as double)", "cast(BuildingArea as double)",
                "cast(YearBuilt as double)")
                .withColumnRenamed("Price", "label");

        fieldsForTraining.show();
        fieldsForTraining.printSchema();
        String[] inputCols = new String[] {
              "Rooms", "Bedroom2", "Bathroom", "Car", "Landsize", "BuildingArea", "YearBuilt"
        };

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(inputCols)
                .setOutputCol("features");

        Dataset<Row> finalDf = assembler.transform(fieldsForTraining);

        finalDf.show();
        finalDf.printSchema();
        Dataset<Row>[] trainTestDf = finalDf.randomSplit(new double[] { 0.8, 0.2});
        Dataset<Row> tainDf = trainTestDf[0];
        Dataset<Row> testDf = trainTestDf[1];

        LinearRegression lr = new LinearRegression();
        LinearRegressionModel trainedModel = lr.train(tainDf);
        System.out.println(trainedModel.coefficients());
        Dataset<Row> testPredictionsDf = trainedModel.transform(testDf);
        testPredictionsDf.show();
        double evaluator = new RegressionEvaluator().setMetricName("rmse")
                .evaluate(testPredictionsDf);
        System.out.println("RMSE = " + evaluator);

    }
}
