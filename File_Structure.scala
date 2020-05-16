/*
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object File_Structure {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val logger = org.apache.log4j.Logger.getLogger("org")
    logger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(this.getlass.getName)
      .config("spark.master", "local[*]").getOrCreate()
++++++++
    val data = spark.read.option("header", "true").option("inferSchema", true) .csv("C:\\Users\\Denzer-Zone\\Desktop\\Abc\\2014_2015.csv")
    data.createOrReplaceTempView("year_file")
    spark.sql("select * from year_file where years='2014'").write.format("csv").save("C:\\Users\\Denzer-Zone\\Desktop\\Abc\\2014_out")
    spark.sql("select * from year_file where years='2015'").write.format("csv").save("C:\\Users\\Denzer-Zone\\Desktop\\Abc\\2015_out")
    data.show()
    data.printSchema()


  }

 */
