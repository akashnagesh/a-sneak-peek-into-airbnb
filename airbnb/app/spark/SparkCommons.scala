package spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.time.Second

/**
  * Created by akashnagesh on 4/18/17.
  */
object SparkCommons {

  lazy val conf = {
    new SparkConf(false)
        .setMaster("local[*]")
    .setAppName("AirBnb")
    .set("spark.logconf","true")
  }


  val sc = SparkContext.getOrCreate(conf)
  val streamingContext = new StreamingContext(sc,Seconds(4))

}
