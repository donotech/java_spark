package com.bdec.training.spark;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.Imputer;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class LogisticRegressionTraining {
    static String diabetesURL = "file:///C:\\Training\\ml_dataset\\diabetes.csv";

    public static Dataset<Row> imputationDemo(Dataset<Row> rawDf, SparkSession spark) {
        String[] colsToImpute = new String[] {"Glucose","BloodPressure","SkinThickness",
                "Insulin","BMI"};
        String[] imputedCols = new String[] {"Glucose_imputed","BloodPressure_imputed",
                "SkinThickness_imputed", "Insulin_imputed", "BMI_imputed"};

        Imputer meanImputer = new Imputer().setStrategy("mean")
                .setMissingValue(0).setInputCols(colsToImpute).setOutputCols(imputedCols);

        Dataset<Row> imputedDataset = meanImputer.fit(rawDf).transform(rawDf);

        imputedDataset.show();

        String[] medianColsToImpute = new String[] {"Pregnancies"};
        String[] medianImputedCols = new String[] {"Pregnancies_imputed"};
        Imputer medianImputer = new Imputer().setStrategy("median")
                .setMissingValue(0).setInputCols(medianColsToImpute)
                .setOutputCols(medianImputedCols);

        Dataset<Row> medianImputedDataset = medianImputer
                .fit(imputedDataset).transform(imputedDataset);

        medianImputedDataset.show();


        //add the columns to encode;
        String[] fieldsToEncode = new String[] {};
        String[] encodedFields = new String[] {};
        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCols(fieldsToEncode)
                .setOutputCols(encodedFields);
        Dataset<Row> encodedDataset = encoder.fit(medianImputedDataset)
                .transform(medianImputedDataset);

        return encodedDataset;
    }

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
                .csv(diabetesURL);

        // Dataset<Row> requiredFields = imputationDemo(rawDf, spark);

        Dataset<Row> requiredFields = rawDf.na().drop();

            Dataset<Row> fieldsForTraining = requiredFields.selectExpr(
                "cast(Pregnancies as double)", "cast(Glucose as double)",
                "cast(BloodPressure as double)", "cast(SkinThickness as double)",
                        "cast(Insulin as double)",
                "cast(BMI as double)", "cast(DiabetesPedigreeFunction as double)",
                "cast(Age as double)", "cast(Outcome as double)")
                .withColumnRenamed("Outcome", "label");

        fieldsForTraining.show();
        fieldsForTraining.printSchema();
        String[] inputCols = new String[] {
              "Pregnancies","Glucose","BloodPressure","SkinThickness",
                "Insulin","BMI","DiabetesPedigreeFunction","Age"
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

        LogisticRegression lr = new LogisticRegression();
        LogisticRegressionModel trainedModel = lr.train(tainDf);
        System.out.println(trainedModel.coefficients());
        Dataset<Row> testPredictionsDf = trainedModel.transform(testDf);
        testPredictionsDf.show();
        double evaluator = new BinaryClassificationEvaluator()
                .evaluate(testPredictionsDf);
        System.out.println("accuracy = " + evaluator);

    }
}
