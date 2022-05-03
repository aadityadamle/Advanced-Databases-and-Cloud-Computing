import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  val start_id = 14701391
  val max_int = Int.MaxValue
  val iterations = 5
 
  def main ( args: Array[ String ] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    val graph 
         = sc.textFile(args(0))
           .map( line => { val e = line.split(","); (e(1).toInt, e(0).toInt) } )   // create a graph edge (i,j), where i follows j

    var R             // initial shortest distances
         = graph.groupByKey()
           .map( a => {if(a._1 == start_id || a._1==1){(a._1,0)}  else{(a._1, max_int)}}  )   // starting point has distance 0, while the others max_int

    for (i <- 0 until iterations) {
       R = R.join(graph)
         .flatMap ( a => {if (a._2._1 == max_int) {List((a._1,a._2._1))} else {List((a._2._2, a._2._1 + 1), (a._1,a._2._1)) } } )   // calculate distance alternatives
         .reduceByKey( (a,b) => {if (a>=b) {b} else {a} } ) // for each node, find the shortest distance
    }

    R.filter( a => a._2 != max_int )     // keep only the vertices that can be reached
      .map( a => (a._2, 1) )      // prepare for the reduceByKey
     .reduceByKey( _+_ )    // for each different distance, count the number of nodes that have this distance
     .sortByKey()
     .collect()
     .foreach(println)
  }
}
