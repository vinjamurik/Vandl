object S3Logs {
	import org.elasticsearch.spark._
	import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf

	var sc:SparkContext = null

  case class S3Log(
     val bucketOwner:String= "",
     val bucket:String = "",
     val time:String = "",
     val remoteIp:String = "",
     val requester:String = "",
     val requestId:String = "",
     val operation:String = "",
     val key:String = "",
     val requestUri:String = "",
     val httpStatus:Int = 0,
     val errorCode:String = "",
     val bytesSent:Int = 0,
     val objectSize:Int = 0,
     val totalTime:Int= 0,
     val turnAroundTime:Int = 0,
     val referrer:String = "",
     val userAgent:String = "",
     val versionId:String = "",
     val _source:String = ""
  )

  def getLogObjectFrom(logLine:String) = {
    try{
      val m = """(\S+?) (\S+?) \[(.+?)\] (\S+?) (\S+?) (\S+?) (\S+?) (\S+?) "(.+?)" (\S+?) (\S+?) (\S+?) (\S+?) (\S+?) (\S+?) "(.+?)" "(.+?)" (\S+?)""".r.findFirstMatchIn(logLine).get
      S3Log(m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6),m.group(7),m.group(8),m.group(9),m.group(10).toInt,m.group(11),m.group(12).toInt,m.group(13).toInt,m.group(14).toInt,
        m.group(15).toInt,m.group(16),m.group(17),m.group(18),logLine)
    }catch{
      case e:Exception => S3Log(_source=logLine)
    }
  }

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId","AKIAJUFNNYKM4OFFQLBQ")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","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")
  }

  def exec() = {
  	sc.textFile(s"s3n://x-globe-logs/content-logs/*").map((x:String) => getLogObjectFrom(x)).saveToEs("xid_s3_logs/default")
  }
}