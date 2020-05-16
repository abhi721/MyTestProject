import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val logger = org.apache.log4j.Logger.getLogger("org")
    logger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("ac")
      .config("spark.master", "local[*]").getOrCreate()
    val data = spark.read.option("header", "true").option("inferSchema", true) .csv("C:\\Users\\Denzer-Zone\\Desktop\\Abc\\2014_2015.csv")
    data.show(5)
    data.createOrReplaceTempView("year_file")
    data.foreach(row=>{
      val id = row(0)
      println(id)
      val ac =row(1)
      val bc = row(2)
    })

  }
}
