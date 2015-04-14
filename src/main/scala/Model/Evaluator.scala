package model

import java.sql.Timestamp

import base.UI
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by lenovo on 2015/4/14
 * for evaluating result.
 */
object Evaluator {

  def run(predictSet: Array[UI], testSet: Array[UI])={
    val eva = evaluate(predictSet,testSet)
    outputParameters(eva._1,eva._2)
  }
  def evaluate(predictSet: RDD[UI], testSet: RDD[UI]): (Double, Double) = {
    val interSet = predictSet.intersection(testSet)

    val interNum = interSet.count().toDouble
    val tS = testSet.count().toDouble
    val rS = predictSet.count().toDouble
    (interNum / tS, interNum / rS)
  }
  def evaluate(predictSet: Array[UI], testSet: Array[UI]): (Double, Double) = {
    val interSet = predictSet.intersect(testSet)

    val interNum = interSet.size.toDouble
    val tS = testSet.size.toDouble
    val rS = predictSet.size.toDouble
    (interNum / tS, interNum / rS)
  }
  def outputParameters(recall: Double, precision: Double): Unit = {
    println("recall = " + recall)
    println("precision = " + precision)
    println("F1 = " + 2*recall*precision/(recall+precision))
  }

  def outputRating(predictSet: RDD[Rating]) = {
    println("user_id,item_id")
    predictSet.collect().toList.foreach(x =>
      println(x.user + "," + x.product))
  }
}
