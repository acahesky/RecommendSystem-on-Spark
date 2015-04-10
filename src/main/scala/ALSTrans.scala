  /**
  * Created by NelsonWang on 2015/1/27.
  * build for test collaborative filtering algorithm
  */

  import java.sql.Timestamp

  import org.apache.spark.SparkContext._
  import org.apache.spark.mllib.recommendation.{ALS, Rating}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}
  case class RatingWithDate(userId: Long,itemId: Long,rating:Double,date: Timestamp) extends serializable

  object ALSTrans {


    val timeDecayFactor = 4.0
    val buyThreshold = 1

    val predictSum = 2000
    val kVal = 30
    val itemVal = 30
    val nodeCapacity = 1000
    val defaultSetSize = 1000000
    val overHot = 10009

    val startTime = Timestamp.valueOf("2014-12-1 0:0:0")
    val timeDivision = Timestamp.valueOf("2014-12-20 0:0:0")


    def main(args: Array[String]) {

      val conf = new SparkConf().setAppName("ALSTrans")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
      @transient val userInfo = sqlContext.sql( "select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user_filtered").map(x => UserData(x.getLong(0), x.getLong(1),x.getLong(2),x.getString(3),x.getLong(4), x(5).asInstanceOf[Timestamp]))
      //@transient val itemInfo = sqlContext.sql("select item_id,time from hiccup.tc_train_item").map(x => x.getLong(0))
      val dataAdapter = splitRatingbyDate(userInfo, timeDivision)
      dataAdapter(0).cache()
      val uiInfo = dataAdapter(0).groupBy(x => (x.userId,x.itemId)).map(x => Rating(x._1._1.toInt,x._1._2.toInt,ratingCalc(x._2)))

      val outUI = dataAdapter(0)
        .filter(x => x.behaviorType ==4)
        .map(x => ((x.userId,x.itemId),1))
        .reduceByKey((x,y) => x+y)
        .filter(x => x._2 <= buyThreshold)
        .map(x => (x._1._1.toInt,x._1._2.toInt))
        .collect
      val rank = 10
      val numIterations = 20
      val ratings = uiInfo

      val testSet = dataAdapter(1).filter(x => x.behaviorType==4)//.map(x => (x.userId.toInt, List(x.itemId.toInt))).reduceByKey((x, y) => x.union(y))

      val usersProducts = ratings.map { case Rating(user, product, rate) =>
        (user, product)
      }

      val model = ALS.train(ratings,rank,numIterations)

      /**
       * 下面这段是计算MSE的，暂时用不到
       */

      /*val predictions =
        model.predict(usersProducts).map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.join(predictions)
      val MSE = ratesAndPreds.map { case ((cuser, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
      println("Mean Squared Error = " + MSE)
      */
      val originPredictions = model.predict(usersProducts)
        .distinct()
        .sortBy(x => -x.rating)
        .filter(
          x => !outUI.contains((x.user,x.product))
        )
      for(tPredictSum <- 400 to (400,100)) {
//        println()
//        println("predictSum: "+tPredictSum)
        val prediction = sc.parallelize(originPredictions.take(tPredictSum))
        //realPre.map(x => x.user.toString+x.product.toString).saveAsTextFile("hdfs://ns1/hiccup/2015040801")
//        val EVA = evaluate(prediction.map(x => (x.user, x.product)), testSet.map(x => (x.userId.toInt, x.itemId.toInt)).distinct())
//        outputParameters(
//          timeDiv = timeDivision,
//          beforeTimeDiv = dataAdapter(0).count(),
//          afterTimeDiv = dataAdapter(1).count(),
//          userCount = testSet.count(),
//          itemCount = 0,
//          ratingCount = 0,
//          recall = EVA._1,
//          precision = EVA._2
//        )
        outputRating(prediction)
      }
  }

    def calcDValue(s1: Timestamp,s2: Timestamp): Double ={
      val days = Math.abs(s1.getTime-s2.getTime).toDouble/3600000.0/24.0
      1.0/(timeDecayFactor * days.toDouble+1.0)
    }

    def ratingCalc(data:Iterable[UserData]):Double= {
      val ss = data.toList
      val bb = ss.groupBy(x => x.behaviorType).map(x => (x._1,(if (x._2.length> 10) 10 else x._2.length).toDouble)).toMap

      var rating = 0.0
      ss.foreach(
        x => {
          rating += calcDValue(timeDivision,x.time)* (x.behaviorType match {
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

    def evaluate(predictSet: RDD[(Int, Int)], testSet: RDD[(Int, Int)]): (Double, Double) = {
      val interSet = predictSet.intersection(testSet)

      val interNum = interSet.count().toDouble
      val tS = testSet.count().toDouble
      val rS = predictSet.count().toDouble
      (interNum / tS, interNum / rS)
    }

    def outputParameters(timeDiv: Timestamp, beforeTimeDiv: Long, afterTimeDiv: Long, userCount: Long, itemCount: Long, ratingCount: Long, recall: Double, precision: Double): Unit = {
  //    println("timeDiv = " + timeDiv)
  //    println("beforeTimeDiv =" + beforeTimeDiv)
  //    println("afterTimeDiv = " + afterTimeDiv)
  //    println("userCount = " + userCount)
  //    println("itemCount = " + itemCount)
  //    println("ratingCount = " + ratingCount)
      println("recall = " + recall)
      println("precision = " + precision)
      println("F1 = " + 2*recall*precision/(recall+precision))
    }

    def splitRatingbyDate(orgData: RDD[UserData], timeDiv: Timestamp): Array[RDD[UserData]] = {
      Array(
        orgData.filter(x => x.time.before(timeDiv)
            //&&(x.time.after(startTime))
          //&&(x.time.getMonth!=12 &&x.time.getDay!=12)
          ),
        orgData.filter(x => x.time.getTime>= timeDiv.getTime)
      )
    }

    def getTimeDiv(timeSet: RDD[Timestamp]): Timestamp = {
      /**
       * assume the rating is uniform distribution
       */
      val maxTime = timeSet.map(x => x.getTime).max()
      val minTime = timeSet.map(x => x.getTime).min()
  //    println("maxTime:" + new Timestamp(maxTime))
  //    println("minTime:" + new Timestamp(minTime))
  //    println("midTime", new Timestamp(((maxTime - minTime) / 10 * 8) + minTime))
      new Timestamp(((maxTime - minTime) / 10 * 8) + minTime)
    }



    def outputRating(predictSet: RDD[Rating]) = {
      println("user_id,item_id")
      predictSet.collect().toList.foreach(x =>
        println(x.user + "," + x.product))
    }
  }