import org.apache.spark.graphx.{Graph => G, VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object GraphComponents {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Graph")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    /* read input and construct of edges */
    val edges :RDD[Edge[Long]]=sc.textFile(args(0)).map(line => { val (node,neighbour) = line.split(",").splitAt(1)
                                          (node(0).toLong,neighbour.toList.map(_.toLong))})
                                          .flatMap(x=> x._2.map(y=>(x._1,y)))
                                          .map(nodes=>Edge(nodes._1,nodes._2,nodes._1))
     /* Use graph builder G.fromEdges to contruct a graph from RDD ofedges and accessing the mapvertices to change the value of vertex ID(id) */                                    
     val graph: G[Long,Long]= G.fromEdges(edges,"defaultProperty").mapVertices((id,_)=>id)
     /* call graph.pregel and method changes its group number to the minimum group number of its neighbor */
     val connectedComponents = graph.pregel(Long.MaxValue,5)(
         (id,oldGroup,newGroup) => math.min(oldGroup,newGroup),
         triplet=>{
           if(triplet.attr<triplet.dstAttr) {
             Iterator((triplet.dstId,triplet.attr))
           } else if((triplet.srcAttr<triplet.attr)){
             Iterator((triplet.dstId, triplet.srcAttr))
           }
           else{
             Iterator.empty
           }
                      
         }, (i,j) => math.min(i,j)
         )
         /* group the vertices by their group number */
         val output = connectedComponents.vertices.map(graph => (graph._2,1)).reduceByKey((a,b) => (a+b))
         .sortByKey().map(s=>s._1.toString + " " + s._2.toString)
         /* print the group sizes */
         println("Connected Components:");
         output.collect().foreach(println)
  }
}
