package model

/**
 * Created by lenovo on 2015/4/14.
 */
import base._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object RulesFilter {

  val ratio1 = 0.9
  val ratio2 = 0.9
  val ratio3 = 0.9
  //val takeSum = 370
  val takeSum = 100 to (1500,100)

  def filteredByUserFeature(rdd: RDD[UserData])= {

    val count1 = (rdd.count() * ratio1).toInt
    val count2 = (count1 * ratio2).toInt
    val count3 = (count2 * ratio3).toInt

    // 过滤掉转化率过低的User
//    val f1 = rdd.sortBy(x => -x.userFeature.purchaseSum).zipWithIndex().filter(x => x._2 <= count1).map(x => x._1)
//    val f2 = f1.sortBy(x => -x.userFeature.c2).zipWithIndex().filter(x => x._2 <= count2).map(x => x._1)
//    val f3 = f2.sortBy(x => -x.userFeature.c1).zipWithIndex().filter(x => x._2 <= count3).map(x => x._1)

    rdd
  }

  /**
   *
   * 说明规则
   *  前一天加入购物车的项目 且没有被购买
   *  前两天加入购物车的项目 且没有被购买 活跃度更高
   */
  def filterByRegular(rdd:RDD[(UISet,LabeledPoint)],itemList:Array[Int])={
    val uiSet = rdd.map(x => x._1)
    uiSet.filter(x =>{
     val f = x.uiFeature.uiFeatures
      val uf = x.userFeature.userFeatures
      val ife = x.itemFeature
      if(f(0).shoppingcartSum > 0 &&f(0).purchaseSum == 0 && uf(2).purchaseDay > 0)
        true
      else
        false
    }
    )
  }
  def filteredByUIFeatureSet(rdd: RDD[UISet],filterUI:Array[Int])= {

    val filter1 = rdd.filter(x => filterUI.contains(x.ui.item))
    val orderedByRating = filter1.sortBy(y => -y.uiFeature.rating).collect()
//    .filter(x =>x.uiFeature.uiFeatures.apply(4).purchaseSum == 0)
      //.filter(x => x.uiFeature.uiFeatures.apply(0).shoppingcartSum>0&&x.uiFeature.uiFeatures.apply(0).purchaseSum==0)
    val takeS = takeSum.toList.union(List(99999))
    takeS.map(x => orderedByRating.take(x).map(x => x.getUI()))
  }
//  def filteredByUIFeature(rdd: RDD[UserData])= {
//    val uiSet = rdd.flatMap(x => x.getUISet)
//    val s1 = uiSet.filter(x => x.uiFeature.purchase != 1)
////    val s2 = s1.sortBy(x => -x.uiFeature.rating).take(takeSum)
////    s2
//    val orderedByRating = s1.sortBy(y => -y.uiFeature.rating)
//    takeSum.map(x => orderedByRating.take(x))
//  }
}
