///**
// * Created by NelsonWang on 2015/1/27.
// * build for test collaborative filtering algorithm
// */
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkContext, SparkConf}
//import java.sql.Timestamp
//import org.apache.spark.SparkContext._
//import org.apache.spark.mllib.recommendation.ALS
//import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
//
//import scala.util.Random
//
//case class RatingWithDate(userId: Long,itemId: Long,date: Timestamp) extends serializable
//
//case class Rating(userId:Long, itemId: Long) extends serializable
//case class CartesianRating(user1Id: Long,user1Count: Long,user2Id: Long,user2Count: Long)
//case class Similarity(user1Id:Long,user2Id:Long,similarity:Double)
//
//object ALSTrans {
//
//  val kVal = 30
//  val itemVal = 30
//  val nodeCapacity = 1000
//  val defaultSetSize = 1000000
//  val overHot = 10009
//  val timeDivision  = new Timestamp("2014-12-18 0:0:0")
//
//  val minHashVal = 100
//  val hashFactor = 1123
//  def hashFunciton(num: Long): Long = {
//    num % hashFactor
//  }
//
//  def evaluate[T <: (Long,List[Long])](predictSet: RDD[(Long,List[Long])],testSet: RDD[(Long,List[Long])]): (Double,Double) ={
//    val joined = predictSet.join(testSet)
//    println("joinedSize:"+joined.count())
//    val ans = joined.map(
//      x => {
//        ( x._2._1.intersect(x._2._2).size,
//          x._2._1.size,
//          x._2._2.size
//          )
//      }
//    ).reduce(
//        (x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))
//    val interNum = ans._1.toDouble
//    val unionNumT = ans._2.toDouble
//    val unionNumR = ans._3.toDouble
//    (interNum / unionNumT, interNum/unionNumR)
//  }
//
//  def outputParameters(timeDiv : Timestamp, beforeTimeDiv: Long, afterTimeDiv: Long,userCount: Long, movieCount: Long, ratingCount: Long,recall: Double,precision: Double): Unit ={
//    println("kVal = "+kVal)
//    println("nodeCapacity = "+nodeCapacity)
//    println("defaultSetSize = "+defaultSetSize)
//    println("timeDiv = "+timeDiv)
//    println("beforeTimeDiv =" + beforeTimeDiv)
//    println("afterTimeDiv = "+ afterTimeDiv)
//    println("userCount = "+userCount)
//    println("movieCount = "+movieCount)
//    println("ratingCount = "+ratingCount)
//    println("recall = "+recall)
//    println("precision = "+precision)
//  }
//
//  def splitRatingbyDate[T<:RatingWithDate](orgData :RDD[T],timeDiv: Timestamp): Array[RDD[Rating]] = {
//    Array(
//      orgData.filter(x => x.date.before(timeDivision)).map(x => Rating(x.userId,x.itemId)),
//      orgData.filter(x => x.date.after(timeDivision)).map(x => Rating(x.userId,x.itemId))
//    )
//  }
//  def getTimeDiv(timeSet: RDD[Timestamp]): Timestamp={
//    /**
//     * assume the rating is uniform distribution
//     */
//    val maxTime = timeSet.map(x => x.getTime).max()
//    val minTime = timeSet.map(x => x.getTime).min()
//    println("maxTime:"+ new Timestamp(maxTime))
//    println("minTime:"+ new Timestamp(minTime))
//    println("midTime",new Timestamp(((maxTime-minTime)/10*8) + minTime))
//    new Timestamp(((maxTime-minTime)/10*8) + minTime)
//  }
//
//  def getRandomList(range: Long) : List[(Long,Long)]={
//    val oList = (1L to range).toList
//    val rList = Random.shuffle(oList)
//    rList.zipWithIndex.map(x => (x._2.toLong+1,x._1))
//  }
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("ALSTrans")
//    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//    @transient val ratingInfo = sqlContext.sql("select user_id,item_id,time from hiccup.tc_train_user").map(x => RatingWithDate(x.getLong(0), x.getLong(1), x(2).asInstanceOf[Timestamp]))
//    val timeDiv = getTimeDiv(ratingInfo.map(x => x.date))
//    /**
//     * SortedByMovieid
//     */
//
//    //1.calculate the top-k familiar user
//    val dataAdapter = splitRatingbyDate(ratingInfo, timeDiv)
//    dataAdapter(0).cache()
//    import org.apache.spark.mllib.recommendation.{Rating => MLRating}
//    val rank = 10
//    val numIterations = 20
//    val ratings = dataAdapter(0).map((x:Rating)=> MLRating(x.userId.toInt,x.itemId.toInt,1.0))
//    val usersProducts = ratings.map { case MLRating(user, product, rate) =>
//      (user, product)
//    }
//    val model = ALS.train(ratings,rank,numIterations,0.01)
//    val sss=model.predict(usersProducts)
//    sss
//    val predictions =
//      model.predict(usersProducts).map { case MLRating(user, product, rate) =>
//        ((user, product), rate)
//      }
//    val ratesAndPreds = ratings.map { case MLRating(user, product, rate) =>
//      ((user, product), rate)
//    }.join(predictions)
//    val MSE = ratesAndPreds.map { case ((cuser, product), (r1, r2)) =>
//      val err = (r1 - r2)
//      err * err
//    }.mean()
//    println("Mean Squared Error = " + MSE)
//    model.
//    //Save and load model
////    model.save(sc, "myModelPath")
////    val sameModel = MatrixFactorizationModel.load(sc, "myModelPath")
////    count the sum of (userid,userid)
////    calculate the similarity matrix according to th e userInfoWithCount
//  }
//
//
//}
