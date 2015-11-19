object Json {
	import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.elasticsearch.spark.rdd.EsSpark

  var sc:SparkContext = null
  //var sqlContext:SQLContext = null;

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
    //sqlContext = new SQLContext(sc);
  }

  def exec(file:String,indexName:String) = {
    EsSpark.saveJsonToEs(sc.textFile(s"${sc.getConf.get("hadoopBasePath")}$file"),s"$indexName/default")
  }
}