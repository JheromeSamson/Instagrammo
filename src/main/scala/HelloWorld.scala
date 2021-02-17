import bean.{ GitHubDataForSchema}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import utils.Utilities

object HelloWorld {

  val dateDownload = "2018-03-01-0"
  val extensionFile = ".json.gz"

  def main(args: Array[String]): Unit = {

    downloadFile()

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val schema = ScalaReflection.schemaFor[GitHubDataForSchema].dataType.asInstanceOf[StructType]

    val jsonDF = sqlContext.read.json("download\\")

    val newJsonDF = jsonDF.withColumnRenamed("default","type")

    jsonDF.show()


  }


  def downloadFile(): Unit = {
    val u = new Utilities()

    u.fileDownloader(dateDownload)
  }

}