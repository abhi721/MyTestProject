
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
object Scala_1 {
  def main(args: Array[String]): Unit = {
     val spark2 = SparkSession.builder().master("local").appName("windowing").getOrCreate()
    //spark2.
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val logger = org.apache.log4j.Logger.getLogger("org")
    logger.setLevel(Level.WARN)

    import spark2.implicits._
     val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
     val simpleDatadf =simpleData.toDF("employee_name", "department", "salary")
    simpleDatadf.show()
    val windowspac = Window.partitionBy("department").orderBy("salary")
     simpleDatadf.withColumn("row_number",row_number().over(windowspac)).show()
    simpleDatadf.withColumn("rank",rank().over(windowspac)).show()
    simpleDatadf.withColumn("dense_rank",dense_rank().over(windowspac)).show()
    simpleDatadf.withColumn("lag",lag("salary",1).over(windowspac)).show()
    simpleDatadf.withColumn("lead",lead("salary",1).over(windowspac)).show()
  }
}
