package model

import java.sql.Timestamp

import base._
import model.FeatureGenerator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object DT {
  val DTTimeDivision = Timestamp.valueOf("2014-12-17 0:0:0")
  val t1 = Timestamp.valueOf("2014-12-14 0:0:0")
  val t2 = Timestamp.valueOf("2014-12-17 0:0:0")
  val t3 = Timestamp.valueOf("2014-12-18 0:0:0")

  def getDTResult(uidiv1:RDD[UIData],testData:RDD[UIData]) = {
    val feature11 = FeatureGenerator.getFeature(uidiv1,t2)
    val feature22 = FeatureGenerator.getFeature(testData,t3)

    val labelSet = testData.collect()
      .filter(
        x =>
          x.behaviorType == 4
      )
      .map(
        x => UI(x.userId,x.itemId)
      )
    val ui11Feature = FeatureGenerator.getDTFeature(feature11,t2,labelSet)
    val ui22Feature = FeatureGenerator.getDTFeature(feature22,t3,Array())
    val testFeature = ui22Feature.map(x => {
      (x._1,LabeledPoint(x._2.label,x._2.toDense))
    })

    val trainingData = ui11Feature.map(x => {
      (x._1,LabeledPoint(x._2.label,x._2.toDense))
    })

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData.map(x => x._2), numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)


    // Evaluate model on test instances and compute test error
    val prediction = testFeature.map { point =>
      val predict = model.predict(point._2.features)
      (point._1, predict,point._2.features.apply(0))
    }.filter(x => x._2 ==1)
      .map(x => x._1)
    prediction
  }

}