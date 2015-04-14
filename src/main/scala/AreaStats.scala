
    import java.sql.Timestamp
    import base._
    import org.apache.spark.{SparkConf, SparkContext}

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

        val x1 = userData.filter(x => x.behaviorType == 4 && x.time.after(timeDivision)).map(x => (x.itemId,x.userGeohash)).join(itemInfo.map(x => (x.item_id,x.item_geohash))).filter(x => x._2._1 == x._2._2).count()

        println("same geohash:"+x1)

      }
    }