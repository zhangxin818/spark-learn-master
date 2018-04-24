package rdd

import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}
import utils.VehicleRecord

/**
  * Author: cwz
  * Time: 2018/3/20
  * Description: 每两辆车相遇的次数，不计算相遇次数为0的。
  */
object MeetCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PassbyCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("spark-learn-master/data/newjson.json")
      .mapPartitions(lines => {
        val gson = new Gson()
        lines.map(gson.fromJson(_, classOf[VehicleRecord]))
      })
        .map(r => (r.getPlaceId, (r.getEid, r.getTime)))

    rdd1.join(rdd1).filter( r => (r._2._1._1 != r._2._2._1) &&(r._2._1._2 - r._2._2._2 <  60) &&(r._2._1._2 - r._2._2._2 >  -60)).map(r => ((r._2._1._1, r._2._2._1),1)).reduceByKey(_ + _).foreach(println(_))

//      rdd1.join(rdd2).filter(r => (r._2._1._1 != r._2._2._1) && (r._2._1._2 - r._2._2._2 <  60) && (r._2._1._2 - r._2._2._2 >  -60)).map(r => ((r._2._1._1, r._2._2._1),1)).reduceByKey(_ + _).foreach(println(_))
//    rdd3.collect()
    sc.stop()
  }

}
