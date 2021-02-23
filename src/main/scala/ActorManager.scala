import bean.{Actor, GitHubData}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ActorManager(val actorRdd: RDD[GitHubData]) {


  def createDataFrame(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)



    //sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(actorRdd), createDataFrameStruct())
    }


  def createDataFrameStruct(): StructType = StructType(Seq(
    StructField("id", StringType, true),
    StructField("login" ,StringType , true),
    StructField("display_login", StringType, true),
    StructField("gravatar_id", StringType, true),
    StructField("url", StringType, true),
    StructField("avatar_url", StringType, true)
  ))




}
