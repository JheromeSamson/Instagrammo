
import bean.{Actor, Author, GitHubData, GitHubDataForSchema, Payload}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.Utilities
import org.apache.spark.sql.functions.{col, count, explode, hour, lit, max, min, minute, second, size, sum, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object HelloWorld {

  val dateDownload = "2018-03-01-0"
  val extensionFile = ".json.gz"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")



    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    /** DECOMMENTARE SE SI VUOLE SCARICARE EVENT CON DATA DIVERSA**/
    //downloadFile(sc,args )

    val jsonDF= sqlContext.read.json(s"C:\\Users\\jhero\\IdeaProjects\\BigData\\download\\${dateDownload}${extensionFile}")

    val schema = ScalaReflection.schemaFor[GitHubDataForSchema].dataType.asInstanceOf[StructType]

    //val jsonDF = sqlContext.read.json(s"download\\")

    val newJsonDF1 =
      jsonDF.withColumnRenamed("public","publicField")
            .withColumnRenamed("default","type")

    createTableFromJson(newJsonDF1, sqlContext)

    val data : Dataset[Row] = sqlContext.sql("select * from DataExtracted").limit(500).toDF()
    import sqlContext.implicits._

    val rdd : RDD[GitHubData] = data.as[GitHubData].rdd

    new GitHubManager(data, sqlContext).manager()



    //author actor rdd
    val author_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("payload.commits").rdd

    //repo rdd
    val repo_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("repo").rdd

    //getting type rdd
    val type_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("type").rdd

    //rdd.foreach(f => println(f))
/*
    val jsonSeq = Seq(newJsonDF)

    val rdd = sqlContext.sparkContext.parallelize(jsonSeq)

    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("titolo", StringType, false),
      StructField("pagine", IntegerType, false),
      StructField("copertina", StringType, false),
      StructField("anno", StringType, false),
      StructField("autore", IntegerType, false)))

*/
  }

  def createTableFromJson(json : DataFrame, sqlContext: SQLContext): Unit = {
    json.registerTempTable("DataExtracted")
  }


  def downloadFile(sc: SparkContext, args: Array[String]): Unit = {
    val u = new Utilities()

    u.fileDownloader(dateDownload)

  }


}
