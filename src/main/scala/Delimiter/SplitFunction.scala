package Delimiter

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object SplitFunction {
  //Create conf object

  val conf = new SparkConf()
    .setAppName("SplitFunction")
    .setMaster("local")

  //create spark context object
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  //import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    println("Split function with variable delimiter per row ")

  }
    def SplitFunction {
      val dept = Seq(
        ("50000.0#0#0#", "#"),
        ("0@1000.0@", "@"),
        ("1$", "$"),
        ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
      dept.show()
      val solution = dept.withColumn("delimiter",concat(lit("\\"),$"delimiter"))
        .withColumn("split_values",expr("split(values,delimiter)"))
      solution.show(truncate=false)
    }
    SplitFunction


    //stop the spark context
    sc.stop
}
