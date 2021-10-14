package NewPackage

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count
import org.apache.spark.{SparkConf, SparkContext}

object AddingCountToDS {
  //Create conf object
  val conf = new SparkConf()
    .setAppName("AddingCount")
    .setMaster("local")

  //create spark context object
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

  def main(args: Array[String]) {
    println("Adding count to a DataSet ")
  }

  def AddingCount {
    val input = Seq(
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604900", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604899", "10.0.0.2.54880", "10.0.0.3.5001", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2),
      ("05:49:56.604908", "10.0.0.3.5001", "10.0.0.2.54880", 2)).toDF("column0", "column1", "column2", "label")

    input.show()
    val sub = Window.partitionBy("column1", "column2")
    val solution = input.withColumn("count", count($"label") over sub)

    solution.show()
  }

  AddingCount

  //stop the spark context
  sc.stop
}
