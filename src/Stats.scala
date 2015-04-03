/**
 * Created by NelsonWang on 2015/1/27.
 * build for test collaborative filtering algorithm
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.Timestamp
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import scala.util.Random
import org.apache.spark.mllib.recommendation.Rating
case class RatingWithDate(userId: Long,itemId: Long,date: Timestamp) extends serializable

//case class Rating(userId:Long, itemId: Long) extends serializable
case class CartesianRating(user1Id: Long,user1Count: Long,user2Id: Long,user2Count: Long)
case class Similarity(user1Id:Long,user2Id:Long,similarity:Double)

object ALSTrans {

  val kVal = 30
  val itemVal = 30
  val nodeCapacity = 1000
  val defaultSetSize = 1000000
  val overHot = 10009
  val timeDivision = Timestamp.valueOf("2015-12-18 0:0:0")

  val minHashVal = 100
  val hashFactor = 1123

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ALSTrans")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val ratingInfo = sqlContext.sql("select user_id,item_id,time from hiccup.tc_train_user").map(x => RatingWithDate(x.getLong(0), x.getLong(1), x(2).asInstanceOf[Timestamp]))
    @transient val itemInfo = sqlContext.sql("select item_id,time from hiccup.tc_train_item").map(x => x.getLong(0))


  }

}