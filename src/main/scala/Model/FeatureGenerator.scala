package model

  /**
  * Created by NelsonWang on 2015/1/27.
  * build for test collaborative filtering algorithm
   * 时间样本占据了很大的评分，因此需要加上时间参数
   * 根据协同过滤计算评分，加至feature中
  */
  import java.sql.Timestamp

  import base._
  import org.apache.spark.mllib.recommendation.{ALS, Rating}
  import org.apache.spark.rdd.RDD

  object FeatureGenerator {


    val timeDecayFactor = 4.0
    val buyThreshold = 1

    def getFeature(userDataRDD:RDD[UIData],endingTime:Timestamp) = {

      val uiList = userDataRDD.groupBy(x => (x.userId,x.itemId))
      val uiInfo = uiList.map(x => Rating(x._1._1.toInt,x._1._2.toInt,ratingCalc(x._2,endingTime)))

      val userFeature = getUserFeature(userDataRDD)
      val outUI = userDataRDD
        .filter(x => x.behaviorType ==4)
        .map(x => ((x.userId,x.itemId),1))
        .reduceByKey((x,y) => x+y)
        .filter(x => x._2 <= buyThreshold)
        .map(x => (x._1._1.toInt,x._1._2.toInt))
        .collect()

      val rank = 10
      val numIterations = 20
      val ratings = uiInfo


      val usersProducts = ratings.map { case Rating(user, product, rate) =>
        (user, product)
      }

      val model = ALS.train(ratings,rank,numIterations)


      val originPredictions = model.predict(usersProducts)
        .distinct()
        .filter( x => !outUI.contains((x.user,x.product)))

      val withRating =uiList
        .join(originPredictions.map(x => ((x.user,x.product),x.rating)))
        .map(x => UISet(UI(x._1._1,x._1._2),UIFeature(x._2._2),x._2._1.toList))

      val userInfo = withRating
        .groupBy(x => x.ui.user)
        .join(userFeature)
        .map(x => UserData(x._1,x._2._2,x._2._1.toList))
      userInfo

  }

    def calcDValue(s1: Timestamp,s2: Timestamp): Double ={
      val days = Math.abs(s1.getTime-s2.getTime).toDouble/3600000.0/24.0
      1.0/(timeDecayFactor * days.toDouble+1.0)
    }

    def ratingCalc(data:Iterable[UIData],endingTime:Timestamp):Double= {
      val ss = data.toList
      val bb = ss.groupBy(x => x.behaviorType).map(x => (x._1,(if (x._2.length> 10) 10 else x._2.length).toDouble)).toMap

      var rating = 0.0
      ss.foreach(
        x => {
          rating += calcDValue(endingTime,x.time)* (x.behaviorType match {
                case 1 => 0.5 * (1.0/Math.sqrt(if(bb.contains(1)) bb(1) else 0))
                case 2 => 3.0 * (1.0/Math.sqrt(if(bb.contains(2)) bb(2) else 0))
                case 3 => 5.0 * (1.0/Math.sqrt(if(bb.contains(3)) bb(3) else 0))
                case 4 => 10.0 * (1.0/Math.sqrt(if(bb.contains(4)) bb(4) else 0))
                case _ => 0.0
              }
          )
        }
      )
//      if(bb.contains(4)&&bb(4)==1)
//        rating = 0.0
      rating
    }




    def getUserFeature(rdd: RDD[UIData])= {
      rdd.groupBy(x => x.userId)
        .map(
          x => {
            val userList = x._2
            val browseSum = userList.count(x => x.behaviorType == 1)
            val collectSum = userList.count(x => x.behaviorType == 2)
            val shoppingcartSum = userList.count(x => x.behaviorType == 3)
            val purchaseSum = userList.count(x => x.behaviorType == 4)
            val conversion1 = browseSum.toDouble/purchaseSum.toDouble
            val conversion2 = collectSum.toDouble/purchaseSum.toDouble
            val conversion3 = shoppingcartSum.toDouble/purchaseSum.toDouble
            val conversion = (browseSum+collectSum+shoppingcartSum).toDouble/purchaseSum.toDouble
            (
              x._1,UserFeature(
              c = conversion,
              c1 = conversion1,
              c2 = conversion2,
              c3 = conversion3,
              shoppingcartSum = shoppingcartSum
            )
              )
          }
        )
  }

  }