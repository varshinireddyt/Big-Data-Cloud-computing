import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

case class M_matrix (i : Int, j: Int, v: Double) 
extends Serializable {}
case class N_matrix (j : Int, I: Int, w: Double) 
extends Serializable {}
object Addition {
  def main(args: Array[ String ]) {
   val conf = new SparkConf().setAppName("Graph")
   conf.setMaster("local[2]")
   val sc = new SparkContext(conf)
   val M = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                M_matrix(a(0).toInt,a(1).toInt,a(2).toDouble)
                                                                   }) 
   val N = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                N_matrix(a(0).toInt,a(1).toInt,a(2).toDouble)
                                                                   }) 
   Val N_T = N.map{case((N._j,(N._i, N._w)) => (N._i,(N._j,N._w))}
   val add = M.map(M => (M._i, (M._j,M._v))).join(N_T)
                        .map({case (i, (j,v),(j,w)) => ((i,j), v+w)})
			.reduceByKey(_+_)
			.sortByKey()
  }
}


