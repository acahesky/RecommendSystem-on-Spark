package base

import java.sql.Timestamp

import org.apache.spark.rdd.RDD

/**
 * Created by lenovo on 2015/4/14.
 */
object DataSpliter {

  def splitDatabyMultiDate(orgData: RDD[UIData], timeDivSet: Array[(Timestamp,Timestamp)]): Array[RDD[UIData]] = {
    timeDivSet.map(
      x => orgData.filter(y => y.time.after(x._1)&& y.time.before(x._2))
    )
  }
  def splitDatabySingleDate(orgData: RDD[UIData], timeDiv: Timestamp): Array[RDD[UIData]] = {
    Array(
          orgData.filter(x => x.time.before(timeDiv)),
          orgData.filter(x => x.time.after(timeDiv))
        )
  }
  def splitDatabyRatio(orgData: RDD[UIData], ratio: Double): Array[RDD[UIData]] = {
    val maxTime = orgData.reduce((x,y)=> if(x.time.after(y.time)) x else y).time
    val minTime = orgData.reduce((x,y)=> if(x.time.before(y.time)) x else y).time
    val midTime = new Timestamp(((maxTime.getTime - minTime.getTime)* ratio + minTime.getTime).toLong)
    Array(
      orgData.filter(x => x.time.before(midTime)),
      orgData.filter(x => x.time.after(midTime))
    )
  }
}
