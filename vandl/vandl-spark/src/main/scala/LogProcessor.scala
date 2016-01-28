/**
  * Created by mehtaam on 1/27/2016.
  */
object LogProcessor {
  /**
    * gets an array of tuples representing field-value pairs
    * @param fields the properties in the log line
    * @param log the log line to extract field-value pairs from
    * @param pattern the regex used for field-value extraction
    * @return an array of tuples representing field-value pairs
    */
  def parseLogLine(fields:String,log:String,pattern:String) : Array[(String,String)] = {
    fields.split(",").zip(pattern.r.findFirstMatchIn(log).get.subgroups)
  }
}