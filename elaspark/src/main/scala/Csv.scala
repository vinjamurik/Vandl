object Csv {
	import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD
  import org.elasticsearch.spark._

  var sc:SparkContext = null

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
  }

  def exec(file:String,indexName:String) = {
    var rdd = sc.textFile(s"${sc.getConf.get("hadoopBasePath")}$file")
    var headers = rdd.first.split(',')
    rdd = rdd.subtract(sc.parallelize(rdd.take(1)))
    rdd.map((x:String) => headers.zip(x.split(',')).toMap).saveToEs(s"$indexName/default")
  }
}