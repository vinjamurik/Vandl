object Driver {
  import org.apache.spark.SparkConf

  def main(args:Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.nodes","172.31.82.218")
    conf.set("hadoopBasePath","hdfs://c1-master.ec2.internal:8020")

    args(0) match {
      case "my_sql" => MySql.init(conf);MySql.exec(args(1))
      case "json" => Json.init(conf);Json.exec(args(1),args(2).toLowerCase)
      case "csv" => Csv.init(conf);Csv.exec(args(1),args(2).toLowerCase)
      case "s3_log" => S3Logs.init(conf);S3Logs.exec()
      case x => AgolLogs.init(conf);AgolLogs.exec()
    }
  }
}