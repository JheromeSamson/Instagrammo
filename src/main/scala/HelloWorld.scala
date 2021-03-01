import bean.{Actor, Author, GitHubData, GitHubDataForSchema, Payload}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.Utilities

object HelloWorld {

  val dateDownload = "2018-03-01-0"
  val extensionFile = ".json.gz"

  def main(args: Array[String]): Unit = {

    //downloadFile()

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val schema = ScalaReflection.schemaFor[GitHubDataForSchema].dataType.asInstanceOf[StructType]

    val jsonDF = sqlContext.read.json("download\\")

    val newJsonDF = jsonDF.withColumnRenamed("default","type")
    val newJsonDF1 = newJsonDF.withColumnRenamed("public","publicField")

    createTableFromJson(newJsonDF1, sqlContext)

    val data : Dataset[Row] = sqlContext.sql("select * from DataExtracted")//.limit(2000)

    import sqlContext.implicits._

    val rdd : RDD[GitHubData] = data.as[GitHubData].rdd

    new GitHubManager(newJsonDF1, sqlContext).Actor()



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


  def downloadFile(): Unit = {
    val u = new Utilities()

    u.fileDownloader(dateDownload)
  }

}