/**
* Created by NelsonWang on 2015/1/27.
* build for test collaborative filtering algorithm
*/

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.Timestamp
import org.apache.spark.SparkContext._

import scala.util.Random

case class RatingWithDate(userId: Long,movieId: Long,date: Timestamp) extends serializable

case class Rating(userId:Long, movieId: Long) extends serializable
case class CartesianRating(user1Id: Long,user1Count: Long,user2Id: Long,user2Count: Long)
case class Similarity(user1Id:Long,user2Id:Long,similarity:Double)

object UserBasedCF {

  val kVal = 30
  val itemVal = 30
  val nodeCapacity = 1000
  val defaultSetSize = 1000000
  val defaultKmeansCapacity = 10000
  val overHot = 10009
  val timeDivision = Timestamp.valueOf("2014-12-18 0:0:0")

  val minHashVal = 100
  val hashFactor = 1123
  def hashFunciton(num: Long): Long = {
    num % hashFactor
  }

  def evaluate[T <: (Long,List[Long])](predictSet: RDD[(Long,List[Long])],testSet: RDD[(Long,List[Long])]): (Double,Double) ={
    val joined = predictSet.join(testSet)
    println("joinedSize:"+joined.count())
    val ans = joined.map(
      x => {
        ( x._2._1.intersect(x._2._2).size,
          x._2._1.size,
          x._2._2.size
        )
      }
    ).reduce(
        (x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))
    val interNum = ans._1.toDouble
    val unionNumT = ans._2.toDouble
    val unionNumR = ans._3.toDouble
    (interNum / unionNumT, interNum/unionNumR)
  }

  def outputParameters(timeDiv : Timestamp, beforeTimeDiv: Long, afterTimeDiv: Long,userCount: Long, movieCount: Long, ratingCount: Long,recall: Double,precision: Double): Unit ={
    println("kVal = "+kVal)
    println("nodeCapacity = "+nodeCapacity)
    println("defaultSetSize = "+defaultSetSize)
    println("defaultKmeansCapacity = "+defaultKmeansCapacity)
    println("timeDiv = "+timeDiv)
    println("beforeTimeDiv =" + beforeTimeDiv)
    println("afterTimeDiv = "+ afterTimeDiv)
    println("userCount = "+userCount)
    println("movieCount = "+movieCount)
    println("ratingCount = "+ratingCount)
    println("recall = "+recall)
    println("precision = "+precision)
  }

  def splitRatingbyDate[T<:RatingWithDate](orgData :RDD[T],timeDiv: Timestamp): Array[RDD[Rating]] = {
    Array(
      orgData.filter(x => x.date.before(timeDivision)).map(x => Rating(x.userId,x.movieId)),
      orgData.filter(x => x.date.after(timeDivision)).map(x => Rating(x.userId,x.movieId))
    )
  }
  def getTimeDiv(timeSet: RDD[Timestamp]): Timestamp={
    /**
     * assume the rating is uniform distribution
     */
    val maxTime = timeSet.map(x => x.getTime).max()
    val minTime = timeSet.map(x => x.getTime).min()
    println("maxTime:"+ new Timestamp(maxTime))
    println("minTime:"+ new Timestamp(minTime))
    println("midTime",new Timestamp(((maxTime-minTime)/10*8) + minTime))
    new Timestamp(((maxTime-minTime)/10*8) + minTime)
  }

  def getRandomList(range: Long) : List[(Long,Long)]={
    val oList = (1L to range).toList
    val rList = Random.shuffle(oList)
    rList.zipWithIndex.map(x => (x._2.toLong+1,x._1))
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UserBasedCFWithMinHash")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val ratingInfo = sqlContext.sql("select user_id,item_id,time from hiccup.tc_train_user").map(x => RatingWithDate(x.getLong(0), x.getLong(1), x(2).asInstanceOf[Timestamp]))
    val timeDiv = getTimeDiv(ratingInfo.map(x => x.date))
    /**
     * SortedByMovieid
     */

    //1.calculate the top-k familiar user
    val dataAdapter = splitRatingbyDate(ratingInfo, timeDiv)
    dataAdapter(0).cache()
    //broad cast each elements count? or join it
    val userList = dataAdapter(0).groupBy[Long](
      (x:Rating)=>
        x.userId,
      1000
    ).map(
      x => (
        x._1,
        x._2.map(
          x => x.movieId
        ).toList
        )
    ).cache()

    val testData = dataAdapter(1).groupBy((x:Rating) => x.userId,1000).map(
      x => (
        x._1,
        x._2.map(
          x =>
            x.movieId
        ).toList
        )
    ).cache()
    val shuffled = sc.parallelize(getRandomList(66000))
    val shuffledUserInfo = dataAdapter(0).map(x => (x.movieId,x.userId)).join(shuffled).map(x => Rating(x._2._1,x._2._2))

    dataAdapter(0).unpersist()

    val userInfoAfterMinHash = shuffledUserInfo.groupBy((x:Rating) => x.userId,1000).map(
    x => (
      x._1,
      x._2.toList.sortBy(x => x.movieId).take(minHashVal)
      )
    )
    //movie dimension reduction

    val bcCount = sc.broadcast(userInfoAfterMinHash.map(x => (x._1,x._2.size)).collect().toMap)

    val movieInfo = userInfoAfterMinHash.flatMap((x:(Long,List[Rating])) => x._2)

    val userInfoWithCount = movieInfo.groupBy((x:Rating) => x.movieId,1000).filter(x => x._2.size<overHot).flatMap(
        x =>
          for (
            ri <- x._2.iterator;
            rj <- x._2.iterator
            if ri.userId < rj.userId
          )
          yield {
            ((ri.userId, rj.userId),1L)
          }
      )
    //count the sum of (userid,userid)
    //calculate the similarity matrix according to th e userInfoWithCount
    val userRelation = userInfoWithCount.reduceByKey((x,y)=> x+y,1000).map(
      x => Similarity(
        x._1._1,
        x._1._2,
        x._2.toDouble / (bcCount.value(x._1._1) + bcCount.value(x._1._2) - x._2).toDouble //Jacarrd similiarity
      )
    ).flatMap(
        x =>
          Seq(
            (x.user1Id,List[Similarity](x)),
            (x.user2Id,List[Similarity](Similarity(x.user2Id, x.user1Id, x.similarity)))
          )
      ).reduceByKey((x,y)=>x++:y,100)
      .map(
        x => (
          x._1,
          x._2.sortBy(
            x => -x.similarity
          ).take(kVal)
          )
      ).cache()

    val beforeRecList = userRelation.flatMap(
      x =>
        for(si <- x._2)
        yield(
          si.user2Id,
          si.user1Id
          )
    ).join(userList,1000)

    val recList = beforeRecList.map(x => x._2).reduceByKey((x,y)=>x++:y).
          map(
              x => (
                x._1,
                x._2.groupBy(x => x)
                  .map(x=> (x._1,x._2.size))
                .toList.sortBy(x => -x._2).take(itemVal)
                .map(x => x._1)
            )
        )
        val evaluation = evaluate(recList,testData)
         //sort with orgKey
        outputParameters(timeDiv = timeDiv,
          beforeTimeDiv = dataAdapter(0).count(),
          afterTimeDiv = dataAdapter(1).count(),
          userCount = 0,
          movieCount = ratingInfo.groupBy(x => x.movieId).count(),
          ratingCount=ratingInfo.count(),
          recall = evaluation._1,
          precision = evaluation._2
        )
  }


}
