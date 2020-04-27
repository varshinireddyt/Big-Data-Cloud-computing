import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Histogram")
    val sc = new SparkContext(conf)
    
    val input = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(0).toLong, a(1).toLong,a(2).toLong)})
    
    var col = input.map(e => (1.toLong, e._1, 1.toLong)).map({case (id,uri,count) => ((id, uri), count)}).reduceByKey(_ + _)
    var col2 = input.map(e => (2.toLong, e._2, 1.toLong)).map({case (id,uri,count) => ((id, uri), count)}).reduceByKey(_ + _)
    var col3 = input.map(e => (3.toLong, e._3, 1.toLong)).map({case (id,uri,count) => ((id, uri), count)}).reduceByKey(_ + _)
    
    var output1 = col.map({case ((key, value), count) => (key, value, count) }).foreach(println)
    var output2 = col2.map({case ((key, value), count) => (key, value, count )}).foreach(println)
    var output3 = col3.map({case ((key, value), count) => (key, value, count) }).foreach(println)
    
    sc.stop()
  }
}
