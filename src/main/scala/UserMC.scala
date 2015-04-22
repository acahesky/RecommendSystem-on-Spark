/**
 * Created by lenovo on 2015/4/21.
 */

import model._
import base._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Timestamp

object UserMC {

  val testMode = true
  val parameter = ""
  var orgPath = "hdfs://ns1/hiccup/"
  val mode = "user/"
  //  val mode = ""
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
    //    @transient val userInfo = sqlContext.sql( "select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user_2").map(x => UIData(x.getLong(0).toInt, x.getLong(1).toInt,x.getLong(2).toInt,x.getString(3),x.getLong(4).toInt, x(5).asInstanceOf[Timestamp]))
    //    //@transient val userInfoFiltered = sqlContext.sql( "select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user_filtered").map(x => UIData(x.getLong(0).toInt, x.getLong(1).toInt,x.getLong(2).toInt,x.getString(3),x.getLong(4).toInt, x(5).asInstanceOf[Timestamp]))
    //    @transient val itemInfo = sqlContext.sql( "select item_id from hiccup.tc_train_item").map(x => x.getLong(0).toInt).collect()
    //
    //
    //    val traingSet = DataSpliter.splitDatabyMultiDate(userInfo,Array((t0,t1))).apply(0)
    //
    //    val resultSet = DataSpliter.splitDatabyMultiDate(userInfo,Array((t1,t2))).apply(0).filter(x => x.behaviorType == 4)
    //val resultUI = resultSet.map(x => UI(x.userId,x.itemId)).collect().distinct

    val positive = sc.objectFile[LabeledPoint](s"$orgPath/positive")
    val negative = sc.objectFile[LabeledPoint](s"$orgPath/negative")
    val testData = sc.objectFile[(Int,LabeledPoint)](s"$orgPath/testSet")
    val itemList = sc.objectFile[Int](s"$orgPath/itemList")
    val resultUI = sc.objectFile[UI](s"$orgPath/resultUI")
    val predictSet = DT.getUserDTResult(positive,negative,testData)

    val s1 = predictSet.collect()
    val s2 = resultUI.collect()
    val ss1 = s1.size
    val ss2 = s2.size
    val interSum = s1.intersect(s2).size
    val recall = interSum.toDouble/ss1.toDouble
    val pre = interSum.toDouble/ss2.toDouble
    println("predictSize:"+ss1)
    println("realSize:"+ss2)
    println("recall:"+recall)
    println("precision:"+pre)
  }


  def getTestSet(testDataRDD: RDD[UIData]) = {
    testDataRDD.filter(x => x.behaviorType == 4).map(x => UI(x.userId,x.itemId)).distinct()
  }
}