
import model.{Evaluator, RulesFilter, FeatureGenerator}
import base._
import org.apache.spark.mllib.recommendation. Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Timestamp

object MC {

  val t1ratio = 0.5
  val t2ratio = 0.7
  val t3ratio = 0.99
  val ttime = Timestamp.valueOf("2014-12-18 00:00:00")
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MC")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val userInfo = sqlContext.sql( "select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user_filtered").map(x => UIData(x.getLong(0).toInt, x.getLong(1).toInt,x.getLong(2).toInt,x.getString(3),x.getLong(4).toInt, x(5).asInstanceOf[Timestamp]))
    val adapter = DataSpliter.splitDatabySingleDate(userInfo,ttime)
    val userSet = FeatureGenerator.getFeature(adapter(0),ttime)

    val testSet = getTestSet(adapter(1)).collect()

    val predictSet = RulesFilter.getFilteredRDD(userSet)

    Evaluator.run(predictSet,testSet)

  }
  def outputAnswer(recall: Double, precision: Double) = {
    val f1 = 2 * recall * precision / (recall + precision)
    println("recall:" + recall)
    println("pricesion:" + precision)
    println("F1: " + f1)
  }

  def getTestSet(testDataRDD: RDD[UIData]) = {
    testDataRDD.filter(x => x.behaviorType == 4).map(x => UI(x.userId,x.itemId)).distinct()
  }
}