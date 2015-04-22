
import base._
import model._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RulesMC {

  val testMode = true
  var orgPath = "hdfs://ns1/hiccup/"
//  val mode = "withoutCF/"
    val mode = ""
  def main(args: Array[String]) {
    orgPath += mode
    if(testMode){
      orgPath += "testFeature"
    }
    else
      orgPath += "feature"
    val conf = new SparkConf().setAppName("MC")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val positive = sc.objectFile[LabeledPoint](s"$orgPath/positive")
    val negative = sc.objectFile[LabeledPoint](s"$orgPath/negative")
    val testData = sc.objectFile[(UISet,LabeledPoint)](s"$orgPath/testSet")
    val itemList = sc.objectFile[Int](s"$orgPath/itemList")
    val resultUI = sc.objectFile[UI](s"$orgPath/resultUI")
    val resultUIC = resultUI.collect()
    val predictSet = RulesFilter.filterByRegular(testData, itemList.collect()).map(x =>x.ui).collect

    if(testMode) {
      println("filtered")
      val eva =
        println("takeSum: " + predictSet.size)
        Evaluator.run(predictSet, resultUIC)
      }
    else
      Evaluator.outputRating(predictSet)
  }


  def getTestSet(testDataRDD: RDD[UIData]) = {
    testDataRDD.filter(x => x.behaviorType == 4).map(x => UI(x.userId,x.itemId)).distinct()
  }
}