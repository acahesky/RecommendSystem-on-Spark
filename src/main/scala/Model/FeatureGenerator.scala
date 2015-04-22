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
    val threeDays = Timestamp.valueOf("2014-11-4 0:0:0").getTime-Timestamp.valueOf("2014-11-1 0:0:0").getTime
    val uiFeatureRegular = Array(1,3,7,30)
    val itemFeatureRegular = Array(1,7,30)
    val userFeatureRegular = Array(1,7,30)
    val alsRank = 10
    val alsNumIterations = 20

    def getFeature(userDataRDD:RDD[UIData],endingTime:Timestamp) = {

      val uiList = userDataRDD.groupBy(x => UI(x.userId,x.itemId))


      val userFeature = getUserFeature(userDataRDD,endingTime)
      val itemFeature = getItemFeature(userDataRDD,endingTime)


      //remove cf
      val uiInfo = uiList.map(x => Rating(x._1.user,x._1.item,ratingCalc(x._2,endingTime)))
      val alsRecommendList = getRating(uiInfo)
      val withRating: RDD[(UI, (Iterable[UIData], Double))] =uiList
        .join(alsRecommendList)
        .map(x => (x._1,x._2))
//      val withRating = userDataRDD.groupBy(x => UI(x.userId,x.itemId)).map(x => (x._1,(x._2,0.0)))
      val uiFeature = getUIFeature(withRating,endingTime)

      val uiFeatureF = mergeFeature(userFeature,itemFeature,uiFeature)

      //之后可以继续优化  随机split的时候可以选择相同user
      uiFeatureF

  }
    def mergeFeature(userFeature:RDD[(Int,UserFeatures)],itemFeatures:RDD[(Int,ItemFeatures)],uiFeatures:RDD[(UI,UIFeatures)])={
      val s = uiFeatures.groupBy(x => x._1.user).join(userFeature).flatMap(
        x =>
          x._2._1
            .map(
              y => UISet(y._1,x._2._2,ItemFeatures.zeroVal,y._2)))
      s.groupBy(x => x.ui.item).join(itemFeatures).flatMap(
        x =>
          x._2._1.map(
            y =>
              y.setItemFeature(x._2._2)
          )
      )
    }

    def calcDays(s1: Timestamp,s2: Timestamp): Double ={
       Math.abs(s1.getTime-s2.getTime).toDouble/3600000.0/24.0
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
    def getItemFeature(rdd:RDD[UIData],endingTime: Timestamp) = {
      val itemList = rdd.groupBy(x => x.itemId)
      itemList.map(
        uiDatas => {
          val itemFeaturePerDate = itemFeatureRegular.map(
            daysLen => {
              val uiListF =
                uiDatas._2.filter(x => calcDays(x.time,endingTime) <= daysLen.toDouble)
              val user = uiListF.groupBy(x => x.userId)
              val userSum = user.size
              val purchase = uiListF.filter(x => x.behaviorType == 4)
              val purchaseSum = purchase.size
              val purchaseUsers = purchase.groupBy(x => x.userId).size
              val visit = uiListF.filter(x => x.behaviorType == 1)
              val visitSum = visit.size
              val visitUsers = visit.groupBy(x => x.userId).size
              val collect = uiListF.filter(x => x.behaviorType == 2)
              val collectSum = collect.size
              val collectUsers = collect.groupBy(x => x.userId).size
              val shoppingcart = uiListF.filter(x => x.behaviorType == 3)
              val shoppingcartSum = shoppingcart.size
              val shoppingcartUsers = shoppingcart.groupBy(x => x.userId).size
              val cV = purchaseSum.toDouble/(visitSum+collectSum+shoppingcartSum).toDouble
              val userCV = purchaseUsers.toDouble/(visitUsers+collectUsers+shoppingcartUsers).toDouble
              val regularCustomerRate =
                purchase
                  .groupBy(x => x.userId)
                  .count(
                    x => {
                      val timeList = x._2.map(
                        x => x.time.getTime
                      )
                      if (timeList.max - timeList.min > threeDays/3)
                        true
                      else
                        false
                    }
                  ).toDouble / purchaseUsers.toDouble
              val jumpOutRate = user.count(x => x._2.size == 1).toDouble / userSum
              ItemFeaturePerDate(
                users = userSum,
                purchaseSum = purchaseSum,
                purchaseUsers = purchaseUsers,
                visitSum = visitSum,
                visitUsers = visitUsers,
                collectSum = collectSum,
                collectUsers = collectUsers,
                shoppingcartSum = shoppingcartSum,
                shoppingcartUsers = shoppingcartUsers,
                cV =cV,
                userCV = userCV,
                regularCustomerRate = regularCustomerRate,
                jumpOutRate = jumpOutRate
              )
            }
          )
          (uiDatas._1,ItemFeatures(itemFeaturePerDate))
        }
      )
    }
    def getUIFeature(rdd: RDD[(UI,(Iterable[UIData],Double))],endingTime:Timestamp) ={
      val uiFeatureWithoutUserFeature = rdd.map(
        x => {
          val uiList = x._2._1.toList
          val outTimeList = uiList.map(x => x.time.getTime)
          val lastAccess = calcDays(new Timestamp(outTimeList.max),endingTime)
          val firstAccess = calcDays(new Timestamp(outTimeList.max),endingTime)
          val tDaysLen = Math.abs(lastAccess-firstAccess)
          val uiFeatures = uiFeatureRegular.map(
            daysLen => {

              val userList = uiList.filter(x => calcDays(x.time, endingTime) <= daysLen.toDouble)

              val visitSum = userList.count(x => x.behaviorType == 1)
              val collectSum = userList.count(x => x.behaviorType == 2)
              val shoppingcartSum = userList.count(x => x.behaviorType == 3)
              val purchaseSum = userList.count(x => x.behaviorType == 4)
              val cv = purchaseSum.toDouble / (visitSum + collectSum + shoppingcartSum).toDouble
              val collectCV = purchaseSum.toDouble / collectSum.toDouble
              val shoppingcartCV = purchaseSum.toDouble / shoppingcartSum.toDouble

              val timeList = userList.map(x => x.time)
              val timeNumList = timeList.map(x => x.getTime)
              val daysInterval =
                if(timeNumList.size !=0)
                  calcDays(new Timestamp(timeNumList.max),new Timestamp(timeNumList.min))
                else
                  0
              val activeDay = timeList.map(x => (x.getMonth + 1, x.getDate)).toList.distinct.size
              val activeRate = activeDay.toDouble / daysInterval.toDouble

              val purchaseDay = userList.filter(x => x.behaviorType == 4).map(x => (x.time.getMonth + 1, x.time.getDate)).toList.distinct.size
              val visitDay = userList.filter(x => x.behaviorType == 1).map(x => (x.time.getMonth + 1, x.time.getDate)).toList.distinct.size
              val collectDay = userList.filter(x => x.behaviorType == 2).map(x => (x.time.getMonth + 1, x.time.getDate)).toList.distinct.size
              val shoppingcartDay = userList.filter(x => x.behaviorType == 3).map(x => (x.time.getMonth + 1, x.time.getDate)).toList.distinct.size

              val visitRate = visitDay.toDouble / daysInterval.toDouble
              val collectRate = collectDay.toDouble / daysInterval.toDouble
              val shoppingcartRate = shoppingcartDay.toDouble / daysInterval.toDouble
              val purchaseRate = purchaseDay.toDouble / daysInterval.toDouble
              UIFeaturePerDate(
                visitSum = visitSum,
                collectSum = collectSum,
                shoppingcartSum = shoppingcartSum,
                purchaseSum = purchaseSum,
                cv = cv,
                collectCV = collectCV,
                shoppingcartCV = shoppingcartCV,
                visitDay = visitDay,
                collectDay = collectDay,
                shoppingcartDay = shoppingcartDay,
                purchaseDay = purchaseDay,
                visitRate = visitRate,
                collectRate = collectRate,
                shoppingcartRate = shoppingcartRate,
                purchaseRate = purchaseRate,
                activeDay = activeDay,
                activeRate = activeRate
              )
            }
          )
            (
              x._1,
              UIFeatures(
                rating = x._2._2,
                firstAccess = firstAccess,
                lastAccess = lastAccess,
                daysLen = tDaysLen,
                uiFeatures = uiFeatures
              )
              )
        }
      )
      // join the UserFeature
      uiFeatureWithoutUserFeature
//        .map(x => (x.getUI().user,x))
//        .join(userFeature)
//        .map(
//          x =>
//            x._2._1.setUserFeature(x._2._2)
//        )
    }


    def getUserFeature(rdd: RDD[UIData],endingTime:Timestamp)= {
      rdd.groupBy(x => x.userId)
        .map(
          uiDatas => {

            val outTimeList = uiDatas._2.map(x => x.time.getTime)
            val firstLogin = outTimeList.min
            val lastLogin = outTimeList.max
            val tDaysLen = calcDays(new Timestamp(firstLogin),new Timestamp(lastLogin))
            val userFeatures = userFeatureRegular.map(
              daysLen =>{
                val userList =
                  uiDatas._2.filter(
                    x =>
                      calcDays(x.time,endingTime) <= daysLen.toDouble
                  )

                val visitSum = userList.count(x => x.behaviorType == 1)
                val collectSum = userList.count(x => x.behaviorType == 2)
                val shoppingcartSum = userList.count(x => x.behaviorType == 3)
                val purchaseSum = userList.count(x => x.behaviorType == 4)
                val cv = purchaseSum.toDouble/(visitSum+collectSum+shoppingcartSum).toDouble
                val collectCV = purchaseSum.toDouble/collectSum.toDouble
                val shoppingcartCV = purchaseSum.toDouble/shoppingcartSum.toDouble

                val timeList = userList.map(x => x.time)
                val timeNumList = timeList.map(x => x.getTime)
                val daysInterval =
                  if(timeNumList.size !=0)
                    calcDays(new Timestamp(timeNumList.max),new Timestamp(timeNumList.min))
                  else
                    1
                val activeDay = timeList.map(x => (x.getMonth+1,x.getDate)).toList.distinct.size
                val activeRate = activeDay.toDouble/daysInterval.toDouble

                val purchaseDay = userList.filter(x => x.behaviorType == 4).map(x => (x.time.getMonth+1,x.time.getDate)).toList.distinct.size
                val visitDay = userList.filter(x => x.behaviorType == 1).map(x => (x.time.getMonth+1,x.time.getDate)).toList.distinct.size
                val collectDay = userList.filter(x => x.behaviorType == 2).map(x => (x.time.getMonth+1,x.time.getDate)).toList.distinct.size
                val shoppingcartDay = userList.filter(x => x.behaviorType == 3).map(x => (x.time.getMonth+1,x.time.getDate)).toList.distinct.size

                val visitRate =visitDay.toDouble/daysInterval.toDouble
                val collectRate = collectDay.toDouble/daysInterval.toDouble
                val shoppingcartRate = shoppingcartDay.toDouble/daysInterval.toDouble
                val purchaseRate = purchaseDay.toDouble/daysInterval.toDouble
                UserFeaturePerDate(
                  visitSum = visitSum,
                  collectSum = collectSum,
                shoppingcartSum=shoppingcartSum,
                purchaseSum = purchaseSum,
                cv = cv,
                collectCV = collectCV,
                shoppingcartCV = shoppingcartCV,
                visitDay = visitDay,
                collectDay = collectDay,
                shoppingcartDay = shoppingcartDay,
                purchaseDay = purchaseDay,
                visitRate = visitRate,
                collectRate = collectRate,
                shoppingcartRate = shoppingcartRate,
                purchaseRate = purchaseRate,
                activeDay = activeDay,
                activeRate = activeRate
                )
              }
            )
            (uiDatas._1,UserFeatures(
              firstLogin = firstLogin,
              lastLogin = lastLogin,
              daysLen = tDaysLen,
              userFeatures = userFeatures
            ))
          }
        )
  }
    def getDTFeature(rdd:RDD[UISet],labelSet:Array[UI]) = {
      rdd.map(x => {
        val label = if(labelSet.contains(x.getUI)) 1 else 0
        (x.ui,x.toDTFeature(label))
      })
    }
    def getDTFeatureWithUISet(rdd:RDD[UISet],labelSet:Array[UI]) = {
      rdd.map(x => {
        val label = if(labelSet.contains(x.getUI)) 1 else 0
        (x,x.toDTFeature(label))
      })
    }
    def getUserDTFeatureWithUISet(rdd:RDD[(Int,UserFeatures)],labelSet:Array[Int]) ={
      rdd.map(x => {
        val label = if(labelSet.contains(x._1)) 1 else 0
        (x._1,(label,x._2.toDense))
      })
    }
//    def getDTFeature(rdd:RDD[UserData],labelSet:Array[UI])={
//      rdd.flatMap(x => {
//        // 融合UserFeature,UIFeature,ItemFeature
//
//        x.uiSet.map(
//          y => {
//          val label = if (labelSet.contains(y.ui)) 1 else 0
//          (
//            y.ui,
//            DTFeature(
//            x.userFeature,
//            y.uiFeature,
//            y.itemFeature,
//            label
//          ))
//        })
//      })
//    }
    def getRating(uiInfo:RDD[Rating])={

      val usersProducts = uiInfo.map { case Rating(user, product, rate) =>
        (user, product)
      }

      val model = ALS.train(uiInfo,alsRank,alsNumIterations)
      model
        .predict(usersProducts)
        .map(
          x =>
            (
              UI(x.user,x.product),
              x.rating
              )
      )

    }
  }