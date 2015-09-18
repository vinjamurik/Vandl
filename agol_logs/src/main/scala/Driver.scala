object Driver extends App {
  import java.util.UUID
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.elasticsearch.spark._

  val conf = new org.apache.spark.SparkConf();
  conf.set("es.nodes","172.31.82.218");
  val sc = new SparkContext(conf)

  val accessKey = "AKIAJUFNNYKM4OFFQLBQ";
  val secretKey = "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";
  val awsBucketName = "x-globe-logs";

  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",accessKey);
  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",secretKey);

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
  import hiveContext.implicits._

  //val countries = List("nepal","gabon","guam","guinea","ivory_coast","liberia","mali","nigeria","senegal","sierra","arctic")

  case class ElasticLog(
     val timestamp:String = "19000101",
     val loadBalanceName:String = "-",
     val clientIP:String = "-",
     val serverIP:String = "-",
     val clientProcessingTime:Double = -1,
     val serverProcessingTime:Double = -1,
     val responseProcessingTime:Double = -1,
     val loadBalancerStatusCode:String = "-",
     val serverStatusCode:String = "-",
     val bytesReceived:Long = 0,
     val bytesSent:Long = 0,
     val requestMethod:String = "-",
     val requestURL:String = "-",
     val requestProtocol:String = "-",
     val userAgent:String = "-",
     val sslCipher:String = "-",
     val sslProtocol:String = "-",
     val regionName:String = "-",
     val productName:String = "-"
  )

def getLogObjectFrom(logLine:String) = {
  try{
    val countries = List("nepal","guinea")
    val m  = """^(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+) (\S+) (\S+\s?)" "(.*)" (\S+) (\S+)""".r.findFirstMatchIn(logLine).get
    var m_sub = """/rest/services/([A-z_]+)/([A-z_]+)/""".r.findFirstMatchIn(m.group(13))
    ElasticLog(
      m.group(1).replaceAll("T"," ").replaceAll("Z",""),
      m.group(2),
      m.group(3),
      m.group(4),
      m.group(5).toDouble,
      m.group(6).toDouble,
      m.group(7).toDouble,
      m.group(8),
      m.group(9),
      m.group(10).toLong,
      m.group(11).toLong,
      m.group(12),
      m.group(13),
      m.group(14),
      m.group(15),
      m.group(16),
      m.group(17),
      try{
        m_sub.get.group(1).toLowerCase
      }catch{
        case e:Exception => "-"
      },
      try{
        m_sub.get.group(2)
      }catch{
        case e:Exception => "-"
      }
    )
  }catch{
    case e:Exception => ElasticLog()
  }
}
  
  val rdd = sc.textFile(s"s3n://$awsBucketName/*/*/*/*/*/*/*/*").map((x:String) => getLogObjectFrom(x))
  //rdd.toDF().saveAsTable("elb_logs",org.apache.spark.sql.SaveMode.Overwrite);
  rdd.saveToEs("xid/elb_logs")
}