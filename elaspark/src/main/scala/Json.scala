object Json {
	import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SQLContext
  import org.elasticsearch.spark.sql._

  var sc:SparkContext = null
  var sqlContext:SQLContext = null;

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc);
  }

  def exec(file:String,indexName:String) = {
    sqlContext.jsonFile(s"${sc.getConf.get("hadoopBasePath")}$file").saveToEs(s"$indexName/default")
  }
}