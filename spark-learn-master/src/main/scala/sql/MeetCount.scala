package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author: cwz
  * Time: 2018/3/20
  * Description: 
  */
object MeetCount {
  def main(args: Array[String]) :Unit = {
    val conf  = new SparkConf().setAppName("PassbyCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    val df = sqlcontext.read.format("json").load("spark-learn-master/data/newjson.json")
    val table = df.select("placeId", "eid", "time")
    table.registerTempTable("record")
//    val sql = sqlcontext.sql("select temptable.placeId, temptable.eid, temptable.time  from ( select record.placeId , record.eid , record.time from record) as temptable inner join record on temptable.placeId = record.placeId").collect().foreach(println(_))
    val sql = sqlcontext.sql("select t1.eid, t2.eid, count(*) from record as t1, record as t2 where t1.placeId = t2.placeId and t1.eid != t2.eid and t1.time - t2.time between -60 and 60 group by t1.eid, t2.eid").collect().foreach(println(_))

//    val sq1 = sqlcontext.sql("select placeId, eid, time from ( select placeId , eid , time from record) as temptable" +
//      "  where temptable.placeId = placeId and temptable.eid != eid and abs(temptable.time - time) < 60").collect().foreach(println(_))
    sc.stop()
  }

}
