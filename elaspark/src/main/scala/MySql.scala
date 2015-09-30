object MySql {
	import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD
  import org.elasticsearch.spark._

  var sc:SparkContext = null
  var tableMap = Map[String,List[String]]()
  var mainRdd:RDD[String] = null;

  def parseTableLine(l:String) = {
    val value = "`([0-z_]+)`".r.findFirstMatchIn(l).get.group(1)
    if(l.contains("CREATE TABLE")){
      sc.setLocalProperty("currentTable",value)
      tableMap = tableMap + (value -> List[String]())
    }
    if(l.contains("DEFAULT") || l.contains("NULL")){
      tableMap = tableMap + (sc.getLocalProperty("currentTable") -> tableMap(sc.getLocalProperty("currentTable")).:+(value))
    }
  }

  def isMetaDataLine(l:String) = {
    l.contains("`") && l.contains("DEFAULT") || l.contains("NULL") || l.contains("CREATE TABLE")
  }

  def isInsertLine(l:String,table:String) = {
    l.contains(s"INSERT INTO `$table`")
  }

  def splitInsertIntoPieces(l:String) = {
    val regex = "(VALUES \\()(.+)(\\);)".r
    val splitRegex = "\\),\\(".r
    splitRegex.split(regex.findFirstMatchIn(l).get.group(2))
  }

  def elasticSave(kv:(String,List[String])) = {
    val regex = "'?(((.*?)'?,)|(,'?(.*?)'?,)|(,'?(.*?)))".r
    mainRdd.filter(isInsertLine(_,kv._1)).flatMap(splitInsertIntoPieces(_)).map((x:String) => kv._2.zip(regex.findAllMatchIn(x).map(_.group(3)).toSeq).toMap).saveToEs(s"xid_${kv._1}/default")
  }

  def init(conf:SparkConf) = {
  	sc = new SparkContext(conf)
  }

  def exec(file:String) = {
  	sc.setLocalProperty("hadoopPath",s"hdfs://c1-master.ec2.internal:8020$file")
    mainRdd = sc.textFile(sc.getLocalProperty("hadoopPath"))
    mainRdd.cache();
  	mainRdd.filter(isMetaDataLine(_)).collect.foreach(parseTableLine(_))
  	tableMap.foreach(elasticSave(_))
  }
}