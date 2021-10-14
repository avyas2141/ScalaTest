package LimitingCollectSetFunction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object LimitingCollect_Set {
  //Create conf object

  val conf = new SparkConf()
    .setAppName("CollectSet")
    .setMaster("local")

  //val spark = SparkSession.builder().appName("test").master("local").getOrCreate

  //create spark context object
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {
    println("Limiting the Collect_set standard function ")

  }


    def CollectSet{
      val input = sqlContext.range(50).withColumn("key", $"id" % 5)

      input.show()

      val solution = input.groupBy('key).agg(collect_set('id) as "all").withColumn("only_first_three", slice('all, 1, 3))
      solution.show(false)
    }
    CollectSet

    // stop sc
    sc.stop()
}
