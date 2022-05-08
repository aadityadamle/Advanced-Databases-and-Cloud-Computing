import org.apache.spark._
import org.apache.spark.sql._

object Twitter {

  case class Follows ( user: Int, follower: Int )

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Twitter")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val m1 = spark.sparkContext.textFile(args(0)).map(_.split(",")).map(twitter => Follows(twitter(0).toInt, twitter(1).toInt)).toDF()
    m1.createOrReplaceTempView("M1")
    val r1 = spark.sql("""SELECT value.follower,count(value.follower) c FROM M1 value group by value.follower""")
    r1.createOrReplaceTempView("R1")
    val m2 = spark.sql("""SELECT value.c, count(value.c) b FROM R1 value group by value.c""")
    m2.createOrReplaceTempView("M2")
    val r2 = spark.sql("""SELECT * FROM M2 value order by value.c""")
    r2.createOrReplaceTempView("R2")
    r2.collect().foreach(println)
  }
}
