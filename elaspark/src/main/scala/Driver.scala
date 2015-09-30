object Driver {
  import org.apache.spark.SparkConf

  def main(args:Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.nodes","172.31.82.218")

    args(0) match {
      case "MySql" => MySql.init(conf);MySql.exec(args(1))
      case x => AgolLogs.init(conf);AgolLogs.exec()
    }
  }
}