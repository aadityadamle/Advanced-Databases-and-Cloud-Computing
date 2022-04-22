import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Twitter {
    def main (args: Array[ String ]) {
        val conf = new SparkConf().setAppName("Twitter")
        val context = new SparkContext(conf)

        //First Map and Reduce
        val count = context.textFile(args(0)).map( line => { val a = line.split(","); ( a(1).toInt, 1 ) } ).reduceByKey( _ + _ )

        // Second Map and Reduce
        val res = count.map(p => (p._2, 1)).reduceByKey( _ + _ ).sortBy(_._2, false).collect()
        
        //Print the result
        res.foreach(println)

        context.stop()        

    }    
}
