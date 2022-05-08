import org.apache.spark.graphx.{Graph,Edge,EdgeTriplet,VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PageRank {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val a = 0.85

    // read the input graph
    val es: RDD[(Long,Long)]
        = sc.textFile(args(0))
            .map {  line => val a = line.split(",")
                    (a(1).toLong, a(0).toLong)
                 }

    // Graph edges have attribute values 0.0
    val edges: RDD[Edge[Double]] = es.map(v =>new Edge(v._1, v._2, 0.0))

    // graph vertices with their degrees (# of outgoing neighbors)
    val degrees: RDD[(Long,Int)] = es.map(v => (v._1,1)).reduceByKey(_+_)

    // initial pagerank
    val init = 1.0/degrees.count

    // graph vertices with attribute values (degree,rank), where degree is the # of
    // outgoing neighbors and rank is the vertex pagerank (initially = init)
    val vertices: RDD[(Long,(Int,Double))] = degrees.map(v => (v._1,(v._2, init)))

    // the GraphX graph
    val graph: Graph[(Int,Double),Double] = Graph(vertices,edges,(0,init))
    val n = graph.vertices.count()

    def newValue ( id: VertexId, currentValue: (Int,Double), newrank: Double ): (Int,Double)
      = {
		val o_degree = currentValue._1
		val cur_val = currentValue._2
		val rank = (1-a)/n + (a * newrank) // a = o.85
		(o_degree, rank)
}

    def sendMessage ( triplet: EdgeTriplet[(Int,Double),Double]): Iterator[(VertexId,Double)]
      = {
	//if (triplet.srcAttr._1 <= 0.0) {Iterator.empty} else {Iterator((triplet.dstId, triplet.srcAttr._2 / triplet.srcAttr._1))  } 
      Iterator((triplet.dstId,triplet.srcAttr._2 / triplet.srcAttr._1))
}

    def mergeValues ( x: Double, y: Double ): Double
      = {x + y}

    // calculate PageRank using pregel
    val pagerank = graph.pregel (init,10) (   // repeat 10 times
                      newValue,
                      sendMessage,
                      mergeValues
                   )

    // Print the top 30 results
    pagerank.vertices.sortBy(_._2._2,false,1).take(30)
            .foreach{ case (id,(_,p)) => println("%12d\t%.6f".format(id,p)) }

  }
}
