
    import java.sql.Timestamp
    import base._
    import model.FeatureGenerator
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}
    import model._
    /**
     * Created by lenovo on 2015/4/7.
     */
    object AreaStats{



      def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("AreaStats")
          val timeDivision = Timestamp.valueOf("2014-12-18 0:0:0")
        val sc = new SparkContext(conf)

        val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

        @transient val userData = sqlContext.sql("select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user_filtered").map(x => UIData(x.getLong(0).toInt, x.getLong(1).toInt,x.getLong(2).toInt,x.getString(3),x.getLong(4).toInt, x(5).asInstanceOf[Timestamp]))

        @transient val itemInfo = sqlContext.sql("select item_id,item_geohash,item_category from hiccup.tc_train_item").map(x => ItemData(x.getLong(0).toInt,x.getString(1),x.getLong(2).toInt))

        val adapter = DataSpliter.splitDatabySingleDate(userData,timeDivision)
        val resultSet = adapter(1).filter(x => x.behaviorType == 4).map(x => UI(x.userId,x.itemId)).collect()
        val dateSet =
          (12 to 12)
            .map(x => DT.getDate(11,x))
        .map(x =>
            (x.getDate,DataSpliter.splitDatabyMultiDate(adapter(0),Array((x,timeDivision))).apply(0)))
        .map(x => (x._1,x._2.map(x => UI(x.userId,x.itemId)).distinct.filter(y => resultSet.contains(y)).count))
        .foreach(println)
        //寻找周期性UI
        //根据购买数量\购买最大间隔计算判断是否为周期性UI

//        val x1 = userData.filter(x => x.behaviorType == 4 && x.time.after(timeDivision)).map(x => (x.itemId,x.userGeohash)).join(itemInfo.map(x => (x.item_id,x.item_geohash))).filter(x => x._2._1 == x._2._2).count()
//
//        println("same geohash:"+x1)

      }
      def regularAnalysis(uiData: RDD[UIData])={
        val uiList = uiData
          .filter(x => x.behaviorType == 4)
          .groupBy(x => UI(x.userId,x.itemId))
          .filter(x => x._2.size > 1)
        uiList.map(x => {
          val tui = x._2.toList
          val timeList = tui.map(x => x.time.getTime)
          val maxTime = new Timestamp(timeList.max)
          val minTime = new Timestamp(timeList.min)
          val maxDaysInterval = FeatureGenerator.calcDays(maxTime,minTime)
          val buysum = tui.size
          val activeRate = buysum.toDouble/maxDaysInterval
          val buyByDate = tui.groupBy(x => UI(x.time.getMonth+1,x.time.getDate)).map(x => (x._1,x._2.size))
          val level =
            if(activeRate>= 3.0/7.0)
              1
            else if(activeRate >=1.0/7.0)
              2
            else if(activeRate >=1.0/14.0)
              3
            else
              4
          (buysum,maxDaysInterval,level,buyByDate)
        })
          .filter(x => x._2 >=1.0)
          .groupBy(x => x._3)
          .collect()
          .foreach(x => {
          println("level:"+x._1.toString+" "+x._2.size.toString)
          x._2.foreach(
            x => {
              println("\n----------------------------\n")
              x._4.foreach(print)
            })
        })
      }
    }