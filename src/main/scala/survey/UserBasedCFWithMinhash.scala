///**
//* Created by NelsonWang on 2015/1/27.
//* build for test collaborative filtering algorithm
//*/
//
//import breeze.linalg.Matrix
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkContext, SparkConf}
//import java.sql.Timestamp
//import org.apache.spark.SparkContext._
//
//case class Rating(userId: Long,movieId: Long,rating: Double,date: Timestamp) extends serializable
//case class RatingWithCount(userId:Long, movieId: Long,count:Long) extends serializable
//case class CartesianRating(user1Id: Long,user1Count: Long,user2Id: Long,user2Count: Long)
//case class Similarity(user1Id:Long,user2Id:Long,similarity:Double)
//
//case class Cart(numList: List[Long],cartList: List[(Long,Long)])
//
//object UserBasedCFWithMinhash {
//
//  val kVal = 30
//  val itemVal = 30
//  val nodeCapacity = 1000
//  val defaultSetSize = 1000000
//  val overHot = 10009
//  val minHashVal = 100
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
//  def splitRatingbyDate[T<:Rating](orgData :RDD[T],timeDiv: Timestamp): Array[RDD[T]] = {
//    Array(
//      orgData.filter(x => x.date.before(timeDiv)),
//      orgData.filter(x => x.date.after(timeDiv))
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
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("CFTest150205")
//    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//    @transient val ratingInfo = sqlContext.sql("select user,movie,rating,time from whale.ml_ratings").map(x => Rating(x.getLong(0), x.getLong(1), x.getDouble(2), x(3).asInstanceOf[Timestamp]))
//    val timeDiv = getTimeDiv(ratingInfo.map(x => x.date))
//    val cartesian = sc.accumulator(Cart(List(),List()))
//    /**
//     * SortedByMovieid
//     */
//
//    //1.calculate the top-k familiar user
//    val dataAdapter = splitRatingbyDate(ratingInfo, timeDiv)
//    //broad cast each elements count? or join it
//    val userInfo = dataAdapter(0).groupBy[Long]((x:Rating)=> x.userId,1000)
//    userInfo.cache
//    val userList = userInfo.map(
//      x => (
//        x._1,
//        x._2.map(
//          x => x.movieId
//        ).toList
//        )
//    ).cache()
//
//    val testData = dataAdapter(1).groupBy((x:Rating) => x.userId,1000).map(
//      x => (
//        x._1,
//        x._2.map(
//          x =>
//            x.movieId
//        ).toList
//        )
//    ).cache()
//
//    val userInfoWithSize = userInfo.flatMap(
//      x => {
//        x._2.map(
//          y =>
//            RatingWithCount(y.userId, y.movieId, x._2.size)
//        )
//      }
//    )
//
//    val userInfoWithCount = userInfoWithSize.groupBy((x:RatingWithCount) => x.movieId,1000).filter(x => x._2.size<overHot).flatMap(
//      x =>
//        for (
//          ri <- x._2.iterator;
//          rj <- x._2.iterator
//          if ri.userId < rj.userId
//        )
//        yield {
//          (CartesianRating(ri.userId,ri.count, rj.userId, rj.count),1L)
//        }
//    )
//
//    val userRelation = userInfoWithCount.reduceByKey((x,y)=> x+y,1000).map(
//      x => Similarity(
//        x._1.user1Id,
//        x._1.user2Id,
//        x._2.toDouble / (x._1.user1Count + x._1.user2Count - x._2).toDouble //Jacarrd similiarity
//      )
//    ).flatMap(
//        x =>
//          Seq(
//            (x.user1Id,List[Similarity](x)),
//            (x.user2Id,List[Similarity](Similarity(x.user2Id, x.user1Id, x.similarity)))
//          )
//      ).reduceByKey((x,y)=>x++:y,100).
//      map(
//        x =>
//          (x._1, x._2.sortBy(
//            x => -x.similarity
//          ).take(kVal)
//            )
//      ).cache()
//
//    val beforeRecList = userRelation.flatMap(
//      x =>
//        for(si <- x._2)
//        yield(
//          si.user2Id,
//          si.user1Id
//          )
//    ).join(userList,1000)
//
//    val recList = beforeRecList.map(x => x._2).reduceByKey((x,y)=>x++:y).
//      map(
//        x => (
//          x._1,
//          x._2.groupBy(x => x)
//            .map(x=> (x._1,x._2.size))
//            .toList.sortBy(x => -x._2).take(itemVal)
//            .map(x => x._1)
//          )
//      )
//    val evaluation = evaluate(recList,testData)
//    //sort with orgKey
//    outputParameters(timeDiv = timeDiv,
//      beforeTimeDiv = dataAdapter(0).count(),
//      afterTimeDiv = dataAdapter(1).count(),
//      userCount = 0,
//      movieCount = ratingInfo.groupBy(x => x.movieId).count(),
//      ratingCount=ratingInfo.count(),
//      recall = evaluation._1,
//      precision = evaluation._2
//    )
//  }
//
//
//}
