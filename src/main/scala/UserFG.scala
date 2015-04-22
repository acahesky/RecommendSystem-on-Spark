/**
 * Created by lenovo on 2015/4/21.
 */
/**
 * Created by lenovo on 2015/4/20.
 */

import java.sql.Timestamp

import base._
import model._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object UserFG {
  /**
   * 28号 周五
   */
  val testMode = true

  var repeatDays = 10
  val startDate = DT.getDate(11,18)
  val endDate = DT.getDate(11,27)

  val oneDay = Timestamp.valueOf("2014-12-19 0:0:0").getTime - Timestamp.valueOf("2014-12-18 0:0:0").getTime

  var t0 = Timestamp.valueOf("2014-11-18 0:0:0")
  var t1 = Timestamp.valueOf("2014-12-3 0:0:0")
  var t2 = Timestamp.valueOf("2014-12-4 0:0:0")
  var orgPath = "hdfs://ns1/hiccup/"
  var mode = "user/"
  var ft0 = Timestamp.valueOf("2014-12-1 0:0:0")
  var ft1 = Timestamp.valueOf("2014-12-18 0:0:0")
  var ft2 = Timestamp.valueOf("2014-12-19 0:0:0")

  def main(args: Array[String]) {
    orgPath += mode
    if(!testMode){
      t0 = Timestamp.valueOf("2014-11-18 0:0:0")
      t1 = Timestamp.valueOf("2014-12-4 0:0:0")
      t2 = Timestamp.valueOf("2014-12-5 0:0:0")
      ft0 = Timestamp.valueOf("2014-12-2 0:0:0")
      ft1 = Timestamp.valueOf("2014-12-19 0:0:0")
      ft2 = Timestamp.valueOf("2014-12-20 0:0:0")
      orgPath +="feature/"
    }
    else{
      orgPath +="testFeature"
    }

    val conf = new SparkConf().setAppName("FG")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val userInfo = sqlContext.sql("select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user_2").map(x => UIData(x.getLong(0).toInt, x.getLong(1).toInt,x.getLong(2).toInt,x.getString(3),x.getLong(4).toInt, x(5).asInstanceOf[Timestamp]))
    @transient val itemList = sqlContext.sql("select item_id from hiccup.tc_train_item_2").map(x => x.getLong(0).toInt)

    val tItemList = itemList.collect()
    val trainingSet = DataSpliter.splitDatabyMultiDate(userInfo,Array((t0,t2))).apply(0)

    val resultSet = DataSpliter.splitDatabyMultiDate(userInfo,Array((ft1,ft2))).apply(0).filter(x => x.behaviorType == 4)
    val resultUI = resultSet.map(x => x.userId)

    val trainingData = (0 to repeatDays).map(
      x =>{
        val startTime = new Timestamp(t0.getTime + oneDay * x)
        val endTime = new Timestamp(t1.getTime + oneDay * x)
        val preTime = new Timestamp(t2.getTime + oneDay * x)
        println(startTime)
        println(endTime)
        println(preTime)
        DT.getUserLabledPoint(trainingSet,startTime,endTime,preTime)
      }
    ).reduce((x,y) => x.union(y))
    //val trainingSetFiltered = userInfo.groupBy(x =>x.itemId).join(itemList.map(x => (x, 0))).flatMap( x => x._2._1)
    //val trainingSetFiltered = trainingSet.filter(x => tItemList.contains(x.itemId))
    val trainingSetFiltered = userInfo
    ///不能广播不知道是什么bug

    val negative = trainingData.filter(x => x.label == 0)
    val positive = trainingData.filter(x => x.label == 1)
    val testData = DT.getUserLabledPointWithUI(trainingSetFiltered,ft0,ft1,ft2)
    println(negative.count())
    println(positive.count())
    println(testData.count())

    negative.saveAsObjectFile(s"$orgPath/negative") //负集合
    positive.saveAsObjectFile(s"$orgPath/positive") //正集合
    testData.saveAsObjectFile(s"$orgPath/testSet") //测试feature数据
    itemList.saveAsObjectFile(s"$orgPath/itemList") //item表
    resultUI.saveAsObjectFile(s"$orgPath/resultUI") //结果UI
  }


  def getTestSet(testDataRDD: RDD[UIData]) = {
    testDataRDD.filter(x => x.behaviorType == 4).map(x => UI(x.userId,x.itemId)).distinct()
  }
}