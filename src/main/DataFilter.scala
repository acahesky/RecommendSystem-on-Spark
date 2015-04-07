
import org.apache.spark.{SparkContext, SparkConf}
import java.sql.Timestamp
import org.apache.spark.SparkContext._

/**
 * Created by lenovo on 2015/4/7.
 */
case class UserData(userId:Long,itemId:Long,behaviorType:Long,userGeohash:String,itemCategory:Long,time:Timestamp) extends serializable
case class ItemData(item_id:Long,item:Long,item_geohash:String,item_category:String) extends serializable
object DataFilter{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFilter")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    @transient val userData = sqlContext.sql("select user_id,item_id,behavior_type,user_geohash,item_category,time from hiccup.tc_train_user").map(x => UserData(x.getLong(0), x.getLong(1),x.getLong(2),x.getString(3),x.getLong(4), x(5).asInstanceOf[Timestamp]))
    @transient val itemInfo = sqlContext.sql("select item_id from hiccup.tc_train_item").map(x => (x.getLong(0),0L))
    val afterGroup = userData.groupBy(x => x.itemId).join(itemInfo).flatMap(x => x._2._1)
    val filteredData = afterGroup.map(x => x.userId.toString+","+x.itemId.toString+","+x.behaviorType.toString+","+x.userGeohash+","+x.itemCategory+","+x.time.toString)
    filteredData.saveAsTextFile("hdfs://ns1/hiccup/filteredData")
  }
}