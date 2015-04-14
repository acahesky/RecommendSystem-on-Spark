package model

/**
 * Created by lenovo on 2015/4/14.
 */
import base.{UIFeature, UserData}
import org.apache.spark.rdd.RDD

object RulesFilter {

  val ratio1 = 0.9
  val ratio2 = 0.9
  val ratio3 = 0.9
  val takeSum = 370
  def getFilteredRDD(rdd: RDD[UserData]) = {
    val s1 = filteredByUserFeature(rdd)
    val s2 = filteredByUIFeature(s1)
    s2.map(x => x.getUI())
  }

  def filteredByUserFeature(rdd: RDD[UserData])= {

    val count1 = (rdd.count() * ratio1).toInt
    val count2 = (count1 * ratio2).toInt
    val count3 = (count2 * ratio3).toInt

    rdd.sortBy(x => - x.userFeature.shoppingcartSum).take(100).map(x => x.userFeature.shoppingcartSum).foreach(println)

    rdd.map(x => x.setUISet(x.uiSet.map(y => y.setUIFeature(UIFeature(y.uiFeature.rating*x.userFeature.c)))))

//    // 过滤掉转化率过低的User
//    val f1 = rdd.sortBy(x => -x.userFeature.c3).zipWithIndex().filter(x => x._2 <= count1).map(x => x._1)
//    val f2 = f1.sortBy(x => -x.userFeature.c2).zipWithIndex().filter(x => x._2 <= count2).map(x => x._1)
//    val f3 = f2.sortBy(x => -x.userFeature.c1).zipWithIndex().filter(x => x._2 <= count3).map(x => x._1)
//
//    f3
  }
  def filteredByUIFeature(rdd: RDD[UserData])= {
    rdd.flatMap(x => x.getUISet).sortBy(x => -x.uiFeature.rating).take(takeSum)
  }
}
