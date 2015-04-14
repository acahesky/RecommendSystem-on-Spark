import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.GraphLoader

/**
 * Created by lenovo on 2015/4/14.
 */
object PGT {
  def main(args:Array[String])={
    val conf = new SparkConf().setAppName("GraphXPageRank")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val graph = GraphLoader.edgeListFile(sc, "hdfs://ns1/hiccup/graphx/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("hdfs://ns1/hiccup/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }
}
