object AgolLogs {
	import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import java.util.Date
  import java.text.SimpleDateFormat
  import org.elasticsearch.spark.rdd.EsSpark
  import com.joestelmach.natty._
  import scala.collection.JavaConversions._

	var sc:SparkContext = null

  case class ElasticLog(
     timestamp:Date = new Date(0),
     loadBalanceName:String = "-",
     clientIP:String = "-",
     serverIP:String = "-",
     clientProcessingTime:Double = -1,
     serverProcessingTime:Double = -1,
     responseProcessingTime:Double = -1,
     loadBalancerStatusCode:String = "-",
     serverStatusCode:String = "-",
     bytesReceived:Long = 0,
     bytesSent:Long = 0,
     requestMethod:String = "-",
     requestURL:String = "-",
     requestProtocol:String = "-",
     userAgent:String = "-",
     sslCipher:String = "-",
     sslProtocol:String = "-",
     regionName:String = "-",
     productName:String = "-"
  )

  def getLogObjectFrom(logLine:String) = {
    try{
      val m  = """^(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+) (\S+) (\S+\s?)" "(.*)" (\S+) (\S+)""".r.findFirstMatchIn(logLine).get
      val m_sub = """/rest/services/([A-z_]+)/([A-z_]+)/""".r.findFirstMatchIn(m.group(13))
      //val m_timestamp = "([\\d-]+?)T([\\d:]+?)\\.".r.findFirstMatchIn(m.group(1))
      ElasticLog(
        new Parser().parse(m.group(1)).get(0).getDates.get(0),
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

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId","AKIAJUFNNYKM4OFFQLBQ")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","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")
  }

  def exec() = {
  	EsSpark.saveToEs(sc.textFile(s"s3n://x-globe-logs/elb/*/*/*/*/*/*/*").map((x:String) => getLogObjectFrom(x)),"xid_agol_logs/default")
  }
}