object Csv {
	import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.elasticsearch.spark.rdd.EsSpark

  var sc:SparkContext = null

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
  }

  def exec(file:String,indexName:String,headerString:String = "") = {
    val rdd = sc.textFile(s"${sc.getConf.get("hadoopBasePath")}$file")
    var headers:Array[String] = Array();
    if(headerString.isEmpty){
      headers = rdd.first.split(',')
    }else{
      headers = headerString.split(',')
    }
    EsSpark.saveToEs(rdd.subtract(sc.parallelize(rdd.take(1))).map((x:String) => headers.zip(x.split(',')).toMap),s"$indexName/default")
  }
}