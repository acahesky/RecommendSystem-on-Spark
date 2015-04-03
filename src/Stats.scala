/**
 * Created by NelsonWang on 2015/1/27.
 * build for test collaborative filtering algorithm
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.Timestamp
case class RatingWithDate(userId: Long,itemId: Long,date: Timestamp) extends serializable

//case class Rating(userId:Long, itemId: Long) extends serializable
case class CartesianRating(user1Id: Long,user1Count: Long,user2Id: Long,user2Count: Long)
case class Similarity(user1Id:Long,user2Id:Long,similarity:Double)

object Stats {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ALSTrans")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  @transient val ratingInfo = sqlContext.sql("select user_id,item_id,time from hiccup.tc_train_user").map(x => RatingWithDate(x.getLong(0), x.getLong(1), x(2).asInstanceOf[Timestamp]))
  @transient val itemInfo = sqlContext.sql("select item_id from hiccup.tc_train_item").map(x => x.getLong(0))
  val userCount = ratingInfo.groupBy(x => x.userId).count
  val itemCount = itemInfo.groupBy(x => x).count
  println("userCount"+ userCount)
  println("itemCount"+ itemCount)
}

}