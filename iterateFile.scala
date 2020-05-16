//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.Level
//import org.apache.spark.sql.SparkSession
//
//import scala.reflect.io.File
//
//object iterateFile {
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\hadoop")
//    val logger = org.apache.log4j.Logger.getLogger("org")
//    logger.setLevel(Level.WARN)
//    val spark = SparkSession.builder()
//      .appName(this.getClass.getName)
//      .config("spark.master", "local[*]").getOrCreate()
//
//    val hdfspath = "C:\\Users\\Denzer-Zone\\Desktop\\Abc\\" // your path here
//    val fs = FileSystem.get(new Configuration())
//    val status = fs.listStatus(new Path("C:\\Users\\Denzer-Zone\\Desktop\\Abc\\"))
//    val data = spark.sparkContext.wholeTextFiles("C:\\Users\\Denzer-Zone\\Desktop\\Abc\\*")
//    import spark.implicimmts._
//    val datadf =data.toDF()
//    datadf.show()
//    val df = spark.createDataFrame(data)
//
//   // df.show(false)
//    status.foreach(x=> println(x.getPath))
//
//  }
//}
