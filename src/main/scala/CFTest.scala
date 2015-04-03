///**
//* Created by NelsonWang on 2015/1/27.
//* build for test collaborative filtering algorithm
//*/
//
//import breeze.linalg.Matrix
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.linalg.Vectors
//import java.sql.Timestamp
//
//case class Rating(userId: Long,movieId: Long,rating: Double,date: Timestamp) extends serializable
//
//object CFTest {
//
//  val kValOrgin = 30
//  val nodeCapacity = 1000
//  val defaultSetSize = 1000000
//  val defaultKmeansCapacity = 10000
//
//  def evaluate[T <: (Long,List[Long])](predictSet: Array[T],testSet: Array[T]): (Double,Double) ={
//    val testMap = testSet.toMap
//    var interNum = 0.0
//    var unionNumT = 0.0
//    var unionNumR = 0.0
//    predictSet.foreach(
//      x => {
//        interNum += x._2.intersect(testMap(x._1)).size
//        unionNumT += testMap(x._1).size
//        unionNumR += x._2.size
//      }
//    )
//    (interNum / unionNumT, interNum/unionNumR)
//  }
//  def outputParameters(timeDiv : Timestamp, beforeTimeDiv: Long, afterTimeDiv: Long,userCount: Long, movieCount: Long, ratingCount: Long,recall: Double,precision: Double): Unit ={
//    println("kVal = "+kValOrgin)
//    println("nodeCapacity = "+nodeCapacity)
//    println("defaultSetSize = "+defaultSetSize)
//    println("defaultKmeansCapacity = "+defaultKmeansCapacity)
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
//  def splitRatingbyDate[T<:(Long,List[Rating])](orgData :RDD[T],timeDiv: Timestamp): Array[RDD[(Long, List[Rating])]] = {
//    Array(
//      orgData.map(
//        x =>
//          (x._1,x._2.filter(x =>
//        x.date.before(timeDiv)))),
//      orgData.map(
//        x =>
//          (x._1,x._2.filter(x =>
//            x.date.after(timeDiv))))
//    )
//}
//  def getTimeDiv(timeSet: RDD[Timestamp]): Timestamp={
//    /**
//     * assume the rating is uniform distribution
//     */
//    val maxTime = timeSet.map(x => x.getTime).max()
//    val minTime = timeSet.map(x => x.getTime).min()
//
//    new Timestamp(((maxTime-minTime)/10*8) + minTime)
//  }
//  def main(args: Array[String]){
//    val conf = new SparkConf().setAppName("CFTest150205")
//    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//    @transient val ratingInfo = sqlContext.sql("select user,movie,rating,time from whale.ml_ratings limit 100000").map(x=>Rating(x.getLong(0),x.getLong(1),x.getDouble(2),x(3).asInstanceOf[Timestamp]))
//    val timeDiv = getTimeDiv(ratingInfo.map(x => x.date))
//    val userInfo = ratingInfo.groupBy(x => x.userId).map( x => (x._1,x._2.toList.sortBy(_.movieId)))
//
//    val dataAdapter = splitRatingbyDate(userInfo,timeDiv)
//
//    val userInfoCount = userInfo.count().toInt
//
//    userInfo.unpersist()
//
//    val kMeansCapacity = if(userInfoCount>defaultKmeansCapacity) defaultKmeansCapacity else userInfoCount
//
//    val ratingForTraining = dataAdapter(0)
//    val ratingForKmeans = ratingForTraining.randomSplit(Array(kMeansCapacity.toDouble/userInfoCount.toDouble,(userInfoCount.toDouble-kMeansCapacity.toDouble)/userInfoCount.toDouble))(0)
//
//    /**
//     * userInfoSorted (RDD[List[Rating]]})
//     */
//
//    val trainingSet =
//      ratingForKmeans.map(
//        x=>
//          Vectors.sparse(
//            Rating.maxId,
//            x._2.map(
//              y =>(
//                y.movieId.toInt,y.rating
//                )
//            ).toSeq)
//      ).cache()
//
//    println("trainingDataCount:"+trainingSet.count)
//    /**
//     * RDD[Vector]
//     */
//
//    val clusterNodeSum = userInfoCount/nodeCapacity+1
//    println("trainingSetCount: "+trainingSet.count)
//    println("clusterNodeSum: "+clusterNodeSum)
//    val clusters = KMeans.train(trainingSet,clusterNodeSum,20)
//
//
//    trainingSet.unpersist()
//    //clusters.clusterCenters.foreach( x => println(x.size))
//
//    val dividedData =
//      ratingForTraining.groupBy(
//        x =>
//          clusters.predict(
//            Vectors.sparse(
//              Rating.maxId,
//              x._2.map(
//                y =>(
//                  y.movieId.toInt,y.rating
//                  )
//              ).toSeq
//            )
//          )
//      ).cache()
//
//    println(dividedData.count())
//    //dividedData.collect().foreach(x => println(x._2.size))
//    /**
//     * transfer Vector into correlation matrix
//     */
//    val recList = dividedData.map(
//      x =>
//        calcRecList(x._2.toArray)
//    ).reduce((x,y) => x++y).sortBy(x => x._1)
//    println("User sum:"+recList.size)
//    dividedData.unpersist()
//    val evaluation = evaluate(
//      recList,
//      dataAdapter(1).map(
//        x => (x._1,x._2.map(x => x.movieId))
//      ).collect()
//    )
//    outputParameters(timeDiv = timeDiv,
//      beforeTimeDiv = 0,//ratingInfo.filter(x=>x.date.before(timeDiv)).count,
//      afterTimeDiv = 0,//ratingInfo.filter(x => x.date.after(timeDiv)).count,
//      userCount = userInfoCount,movieCount = ratingInfo.groupBy(x => x.movieId).count(),ratingCount=ratingInfo.count(),
//      recall = evaluation._1,
//      precision = evaluation._2
//    )
//
//    recList.foreach(
//      x => {
//        print("userId:"+x._1+":RecList("+x._2.size+")  ")
//        x._2.foreach(x => print(x+" "))
//        println()
//      }
//    )
//  }
//  def similarityFunction(x: List[Rating], y: List[Rating]): Double = {
//    x.intersect(y).size.toDouble/x.union(y).size.toDouble
//  }
//
//  /**
//   * @param cluster : calculate recommend list for every user in each cluster
//   * @return recommend movie list
//   */
//  def calcRecList(cluster: Array[(Long,List[Rating])]) : Array[(Long,List[Long])]={
//    val n = cluster.size
//    val kVal = if(kValOrgin> n)n else kValOrgin
//    val sMat = Matrix.zeros[Double](n,n)
//    for(i <- 0 until n)
//      for(j <- 0 to i) {
//        sMat.update(i,j,similarityFunction(cluster(i)._2,cluster(j)._2))
//        sMat.update(j,i,sMat(i,j))
//      }
//
//    val sVec = sMat.flatten().toArray.zipWithIndex.map(x => (x._1,x._2%n))
//
//    /**
//    * return (Long,List[movieId: Int])
//        */
//      var i = 0
//      cluster.map(x => (x._1,{
//        val sArr = sVec.slice(n*i,n*i+n).sortBy(x => -x._1).diff(cluster(i)._2.map(x => x.movieId)).take(kVal).map(x => x._2)
//        i = i+1
//        sArr.map(x => cluster(x)._2
//          .map(x => x.movieId)
//      ).reduce((x,y) => x.union(y)).take(kVal)
//    }))
//  }
//}
