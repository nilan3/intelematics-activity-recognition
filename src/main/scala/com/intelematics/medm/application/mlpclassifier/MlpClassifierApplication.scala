package com.intelematics.medm.application.mlpclassifier

import com.intelematics.medm.common.SparkSessionHelper
import com.intelematics.medm.utils.{AppConfig, Utilities}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MlpClassifierApplication(config: AppConfig) extends SparkSessionHelper {
  protected val configuration: AppConfig = config
  protected val spark: SparkSession = createSpark(configuration)
  protected val inputFeatures: List[String] = configuration.getList("model.features")
  protected val outputLabel: String = configuration.getString("model.label")

  def importDf(): DataFrame = {
    val cols: List[String] = inputFeatures :+ outputLabel
    spark
      .read
      .parquet(configuration.getString("hdfs.input"))
      .select(cols.head, cols.tail: _*)
  }

  def fit() = {
    val df = importDf()
    val assembler = new VectorAssembler()
      .setInputCols(inputFeatures.toArray)
      .setOutputCol("nonScaledFeatures")
    val nonScaledFeatures = assembler.transform(df)
    val features = standardScaler(nonScaledFeatures, "nonScaledFeatures", "features")
    val labelIndexer = new StringIndexer().setInputCol(outputLabel).setOutputCol("indexedLabel").fit(features)
    println(s"Found labels: ${labelIndexer.labels.mkString("[", ", ", "]")}")
    val outputLayer = labelIndexer.labels.length
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(inputFeatures.length).fit(features)
    val (trainingData, testData) = split(0.9, 0.1, features)
    val layers = Array[Int](inputFeatures.length, 8, outputLayer)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setBlockSize(128)
      .setSeed(System.currentTimeMillis)
      .setMaxIter(200)
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)
    model.write.overwrite().save(configuration.getString("hdfs.output") + "/model")
    predictions
      .drop("features")
      .drop("indexedLabel")
      .drop("indexedFeatures")
      .drop("rawPrediction")
      .drop("probability")
      .drop("prediction")
      .coalesce(1).write.mode("overwrite")
      .parquet(configuration.getString("hdfs.output") + "/test_results")
    assessAccuracy(predictions)
  }

  def split(train: Double, test: Double, features: DataFrame) = {
    val splits = features.randomSplit(Array(train, test))
    val trainingData = splits(0)
    val testData = splits(1)
    (trainingData, testData)
  }

  def assessAccuracy(predictions: DataFrame) = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Accuracy = " + accuracy)
  }

  def standardScaler(df: DataFrame, inputCol: String, outputCol: String): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setWithStd(true)
      .setWithMean(true)
    val scalerModel = scaler.fit(df)
    scalerModel.write.overwrite().save(configuration.getString("hdfs.output") + "/scaler")
    scalerModel.transform(df)
  }
}

object MlpClassifierApplication {
  def main(args: Array[String]): Unit = {
    val configuration = Utilities.loadConfig(args)
    val trainingPipeline = new MlpClassifierApplication(configuration)
    trainingPipeline.fit()
  }
}