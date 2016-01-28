import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by mehtaam on 1/27/2016.
  */
object Driver {
  /**
    * args must be in following order: {data source},{es nodes},{index/type},{fields},{regex} followed by case-specific args
    * @param args array of string arguments to run this app
    */
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().set("es.nodes",args(1))
    args(0) match {
      case "lake"  => {//{hdfs path}
        //need to specify new conf so that es.nodes can be overridden
        val sc = new SparkContext(conf)
        //to allow connecting to aws
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId","AKIAJUFNNYKM4OFFQLBQ")
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","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")
        //runs the log processor on passed in bucket using passed in fields and regex, and save to passed in index/type
        EsSpark.saveToEs(sc.textFile(s"s3n://${args(5)}").map(LogProcessor.parseLogLine(args(3),_,args(4)).toMap),args(2))
      }
      case "stream" => {//{streaming nodes},{topics}
        val ssc = new StreamingContext(conf,Seconds(1))
        val rdds = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,Map("metadata.broker.list" -> args(5)),args(6).split(",").toSet)
        rdds.foreachRDD(x => EsSpark.saveToEs(x.map(y => LogProcessor.parseLogLine(args(3),y._2,args(4)).toMap),args(2)))
        ssc.start()
        ssc.awaitTermination()
      }
    }
  }
}