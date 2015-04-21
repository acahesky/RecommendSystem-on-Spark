/**
 * Created by lenovo on 2015/4/15.
 */

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import base._
import java.sql.Timestamp
/**
 * Created by lenovo on 2015/4/14.
 */

object PersonalRank {
  val alpha = 0.5
  val maxStep = 2

  def getRecList(graph:Graph[(Double,Int),String],root:Long,alpha: Double,maxStep :Int,recSum: Int) = {

    val initGraph = graph.mapVertices(
      (x,y) =>
        if(x != root)
          y
        else
          (1.0,y._2)
    )


    val resultGraph = loop(initGraph,maxStep)

    val rootRating = resultGraph.collectNeighbors(EdgeDirection.Either).filter(x => x._1 == root).first()._2
    println(rootRating.size)
    rootRating.foreach(println)

    rootRating
      .filter(x => x._1%2 == 1)
      .sortBy(x => -x._2._1)
      .take(recSum)
  }
  def loop(tGraph: Graph[(Double,Int),String],k: Int): Graph[(Double,Int),String]={
    if(k == 1)
      tGraph
    else
    {
      val s = tGraph.aggregateMessages[(Double,Int)](
        triplet  => {
          triplet.sendToSrc((triplet.dstAttr._1/triplet.dstAttr._2.toDouble*alpha,triplet.srcAttr._2))
          triplet.sendToDst((triplet.srcAttr._1/triplet.srcAttr._2.toDouble*alpha,triplet.dstAttr._2))
        },
        (x,y) => {
          (x._1+y._1,x._2)
        }
      )
      loop(Graph(s,tGraph.edges),k-1)
    }
  }
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("PersonalRank")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val uiInfo = sqlContext.sql("select user,movie,rating,time from whale.ml_ratings limit 100000").map(x=>UIData(x.getLong(0).toInt*2,x.getLong(1).toInt*2+1,0,"",0,x(3).asInstanceOf[Timestamp]))

    val adapter = DataSpliter.splitDatabyRatio(uiInfo,0.8)
    val trainingSet = adapter(0)

    val userList = trainingSet
      .groupBy(x => x.userId)
      .map(x => x._1)
    val itemList = trainingSet
      .groupBy(x => x.itemId)
      .map(x => x._1)

    val vertexSet = userList
      .map(x => (x.toLong,0.0))
      .union(itemList.map(x => (x.toLong,0.0)))

    val relationships =
      trainingSet
        .map(x => Edge(x.userId.toLong,x.itemId.toLong,""))

    val graph = Graph(vertexSet,relationships)

    val outDegrees = graph.outDegrees

    val root = userList.top(1).apply(0)

    val vertexSetWithOutDegrees = vertexSet.join(outDegrees)
    val defaultValue = (0.0,0)
    val graphWithDegrees = Graph(vertexSetWithOutDegrees,relationships,defaultValue)

    val recList = getRecList(
      graphWithDegrees,
      root,
      alpha,maxStep
      ,10
    )
    recList.map(x => (x._1/2).toInt).foreach(println)
  }
}
