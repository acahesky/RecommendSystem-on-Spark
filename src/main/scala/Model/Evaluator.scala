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
    outputParameters(eva._1,eva._2,eva._3,eva._4,eva._5)
    eva
  }

  def evaluate(predictSet: RDD[UI], testSet: RDD[UI]): (Long,Long,Double, Double) = {
    val predictSetSum = predictSet.count()
    val testSetSum = testSet.count()
    val interSet = predictSet.intersection(testSet)
    val interNum = interSet.count().toDouble
    val tS = testSet.count().toDouble
    val rS = predictSet.count().toDouble
    (predictSetSum,testSetSum,interNum / tS, interNum / rS)
  }
  def evaluate(predictSet: Array[UI], testSet: Array[UI]): (Long,Long,Double, Double,Double) = {

    val predictSetSum = predictSet.size
    val testSetSum = testSet.size
    val interSet = predictSet.intersect(testSet)
    val interNum = interSet.size.toDouble
    val tS = testSet.size.toDouble
    val rS = predictSet.size.toDouble
    val recall = interNum/tS
    val precision = interNum/rS
    val f1 =  2*recall*precision/(recall+precision)
    (predictSetSum,testSetSum,recall, precision,f1)
  }

  def outputParameters(predictSetSum:Long,testSetSum:Long,recall: Double, precision: Double,f1:Double): Unit = {
    println("predictSum: "+predictSetSum)
    println("testSetSum: "+testSetSum)
    println("recall = " + recall)
    println("precision = " + precision)
    println("F1 = " + f1)
  }

  def outputRating(predictSet: Array[UI]) = {
    println("user_id,item_id")
    predictSet.toList.foreach(x =>
      println(x.user + "," + x.item))
  }
}
