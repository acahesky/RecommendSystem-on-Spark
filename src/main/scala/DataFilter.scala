
import java.sql.Timestamp

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by lenovo on 2015/4/7.
 */
case class UserData(userId:Long,itemId:Long,behaviorType:Long,userGeohash:String,itemCategory:Long,time:Timestamp) extends serializable
case class ItemData(item_id:Long,item:Long,item_geohash:String,item_category:String) extends serializable
object DataFilter{
  val timeDivision = Timestamp.valueOf("2014-12-18 0:0:0")
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFilter")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val userData = sqlContext.sql("select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user").map(x => UserData(x.getLong(0), x.getLong(1),x.getLong(2),x.getString(3),x.getLong(4), x(5).asInstanceOf[Timestamp]))

    val dataAdapter = splitRatingbyDate(userData, timeDivision)
    val testSet2 = dataAdapter(1).filter(x => x.behaviorType==4)
    outputRating(testSet2.map(x => Rating(x.userId.toInt,x.itemId.toInt,0)))
    /**
     * 用join来过滤user中item不在item列表中的项目
    @transient val itemInfo = sqlContext.sql("select item_id from hiccup.tc_train_item").map(x => (x.getLong(0),0L))
    val afterGroup = userData.groupBy(x => x.itemId).join(itemInfo).flatMap(x => x._2._1)
    val filteredData = afterGroup.map(x => x.userId.toString+","+x.itemId.toString+","+x.behaviorType.toString+","+x.userGeohash+","+x.itemCategory+","+x.time.toString)
    filteredData.saveAsTextFile("hdfs://ns1/hiccup/filteredData")
    */
  }

  def splitRatingbyDate(orgData: RDD[UserData], timeDiv: Timestamp): Array[RDD[UserData]] = {
    Array(
      orgData.filter(x => x.time.before(timeDiv)),
      orgData.filter(x => x.time.after(timeDiv))
    )
  }
  def outputRating(predictSet: RDD[Rating]) = {
    println("user_id,item_id")
    predictSet.distinct().sortBy(x => -x.rating).take(2000).toList.foreach(x =>
      println(x.user + "," + x.product))
  }
}