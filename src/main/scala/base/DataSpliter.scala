package base

import java.sql.Timestamp

import org.apache.spark.rdd.RDD

/**
 * Created by lenovo on 2015/4/14.
 */
object DataSpliter {

  def splitDatabyMultiDate(orgData: RDD[UIData], timeDivSet: Array[(Timestamp,Timestamp)]): Array[RDD[UIData]] = {
    timeDivSet.map(x => orgData.filter(y => y.time.after(x._1)&& y.time.before(x._2)))
//    Array(
//      orgData.filter(x => x.time.before(timeDiv)
//        //&&(x.time.after(startTime))
//        //&&(x.time.getMonth!=12 &&x.time.getDay!=12)
//      ),
//      orgData.filter(x => x.time.getTime>= timeDiv.getTime)
//    )
  }
  def splitDatabySingleDate(orgData: RDD[UIData], timeDiv: Timestamp): Array[RDD[UIData]] = {
    Array(
          orgData.filter(x => x.time.before(timeDiv)
            //&&(x.time.after(startTime))
            //&&(x.time.getMonth!=12 &&x.time.getDay!=12)
          ),
          orgData.filter(x => x.time.getTime>= timeDiv.getTime)
        )
  }
}
