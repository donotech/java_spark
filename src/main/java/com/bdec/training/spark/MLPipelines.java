package com.bdec.training.spark;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.util.Arrays;

public class MLPipelines {
    public static void main(String[] args) {
        String winutilPath = "C:\\softwares\\winutils";

        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("Detected windows");
            System.setProperty("hadoop.home.dir", winutilPath);
            System.setProperty("HADOOP_HOME", winutilPath);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
                .master("local[*]")
                .getOrCreate();

        Encoder<NLPPipelineDataset> personEncoder = Encoders.bean(NLPPipelineDataset.class);
        Dataset<NLPPipelineDataset> training = spark.createDataset(
                Arrays.asList(
                        new NLPPipelineDataset(0L, "a b c d e spark", 1.0),
                        new NLPPipelineDataset(1L, "b d", 0.0),
                        new NLPPipelineDataset(2L, "spark f g h", 1.0),
                        new NLPPipelineDataset(3L, "hadoop mapreduce", 0.0)
                ),
                personEncoder
        );

        // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");

//    val tokenizedDf = tokenizer.transform(training)
//    training.show()
//    tokenizedDf.show(truncate = false)
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");

//    val tfDf = hashingTF.transform(tokenizedDf)
//    tfDf.show(truncate = false)

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.001);

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});
//
//    // Fit the pipeline to training documents.
        PipelineModel model = pipeline.fit(training);
        model.transform(training).show();
//
//    // Now we can optionally save the fitted pipeline to disk
        try {
            model.write().overwrite().save("/tmp/spark-logistic-regression-model");
        } catch (IOException e) {
            e.printStackTrace();
        }
//
//    // We can also save this unfit pipeline to disk
//        try {
//            pipeline.write().overwrite().save("/tmp/unfit-lr-model");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
////
////    // And load it back in during production
        PipelineModel sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model");
//
//    // Prepare test documents, which are unlabeled (id, text) tuples.
        Dataset<NLPPipelineDataset> test = spark.createDataset(
                Arrays.asList(
                        new NLPPipelineDataset(4L, "spark i j k", 0.0),
                        new NLPPipelineDataset(5L, "l m n", 0.0),
                        new NLPPipelineDataset(6L, "spark hadoop spark", 0.0),
                        new NLPPipelineDataset(7L, "apache hadoop", 0.0)
                ), personEncoder);

        // Make predictions on test documents.
        sameModel.transform(test)
                .select("id", "text", "probability", "prediction").show();

    }
}
