package survey

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by lenovo on 2015/4/13.
 */
object ANOVA {
  def main(args: Array[String])={

    val conf = new SparkConf().setAppName("ModelController")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  }
}
