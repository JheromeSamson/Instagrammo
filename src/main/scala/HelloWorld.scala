import bean.{Actor, Author, GitHubData, GitHubDataForSchema, Payload}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.Utilities
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.SparkSession

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

    val data = sqlContext.sql("select * from DataExtracted")

    import sqlContext.implicits._

    val rdd = data.as[GitHubData].rdd

    //rdd.foreach(f => println(f))

    val schema_actor = ScalaReflection.schemaFor[Actor].dataType.asInstanceOf[StructType]

    /**  GETTING RDD */

    //getting actor rdd
     val actor_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("actor").rdd

    //author actor rdd
    val author_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("payload.commits").rdd

    //repo rdd
    val repo_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("repo").rdd

    //getting type rdd
    val type_rdd = sqlContext.read.json("download\\2018-03-01-0.json").select("type").rdd

    repo_rdd.foreach(f => println(f))





    //new ActorManager(rdd)

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

    val jsDF = sqlContext.createDataFrame(rdd,schema)

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