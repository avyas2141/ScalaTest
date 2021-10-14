package ScalaWorkshop

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object MostImp {
  val conf = new SparkConf()
    .setAppName("SplitFunction")
    .setMaster("local")

  //create spark context object
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {
    println("Assigned Priority")
  }

    def AssignedPriority{
      val input = Seq(
        (1, "MV1"),
        (1, "MV2"),
        (2, "VPV"),
        (2, "Others")).toDF("id", "value")

      input.show()

      val priorities = Seq(
        "MV1",
        "MV2",
        "VPV",
        "Others").zipWithIndex.toDF("name","rank")
      val sub = input.join(priorities).where($"value" === $"name").groupBy("id").agg(min("rank") as "min")

      sub.show()

      val solution = sub.join(priorities).where($"min" === $"rank").select("id", "name")

      solution.show(truncate=false)
    }
    AssignedPriority

    //stop the spark context
    sc.stop
}
