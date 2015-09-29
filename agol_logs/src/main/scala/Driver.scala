object Driver {
  def main(args:Array[String]) = {
    import java.util.UUID
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.elasticsearch.spark._

    val conf = new org.apache.spark.SparkConf()
    conf.set("es.nodes","172.31.82.218")
    val sc = new SparkContext(conf)

    val accessKey = "AKIAJUFNNYKM4OFFQLBQ"
    val secretKey = "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"
    val awsBucketName = "x-globe-logs"
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",accessKey)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",secretKey);

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
        val m  = """^(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+) (\S+) (\S+\s?)" "(.*)" (\S+) (\S+)""".r.findFirstMatchIn(logLine).get
        val m_sub = """/rest/services/([A-z_]+)/([A-z_]+)/""".r.findFirstMatchIn(m.group(13))
        val m_timestamp = "([\\d-]+?)T([\\d:]+?)\\.".r.findFirstMatchIn(m.group(1))
        ElasticLog(
          m_timestamp.get.group(1)+" "+m_timestamp.get.group(2),
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
    rdd.saveToEs("xid/elb_logs")
  }
}