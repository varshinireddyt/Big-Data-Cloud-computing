import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[ String ]) {
   val conf = new SparkConf().setAppName("Graph")
   conf.setMaster("local[2]")
   val sc = new SparkContext(conf)
   val input = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                   a.map(value =>value.toLong)
                                                                   }) 
   var graph = input.map(node =>
     {(node(0),node(0),node.slice(1,node.length).toList)
     })
     /*pseudo Code */
    for(i <-1 to 5){ 
       graph = graph.flatMap(node => { (node._1,node._2) ::
                            (node._3.map(a => (a,node._2)))
                                             })
       .reduceByKey((i,j) => (if(i < j) i else j)) 
       .join(graph.map(g => {(g._1, (g._2, g._3))}))
       .map(g => {
         (g._1,g._2._1,g._2._2._2)})
       }
    val groupSize = graph.map(g => (g._2, 1)).reduceByKey(_+_).sortBy(_._1,true)
//    groupSize = graph.sortBy(_._1,true)
    groupSize.collect().foreach(println)
  }
}


