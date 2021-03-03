
import bean.{Actor, GitHubData}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class GitHubManager(val dataFrame : sql.DataFrame, val SQLContext: SQLContext) {

  var rdd = dataFrameToRDD()

  def Actor(): Unit = {


    //var dataframe : sql.DataFrame = ActorManagerRddToDataFrame()

    //val pairRdd : RDD[(String, Actor)] = ActorManagerRddToPairRdd()

    //var rddActor : RDD[Actor] = ActorManagerRddToRdd()

    //var dataset : Dataset[Actor] = ActorManagerRddToDataSet(dataframe)

    //new ManagerRDD(rdd, SQLContext)

    new ManagerDATAFRAME(dataFrame, SQLContext).NumeroCommit()

    new ManagerRDD(rdd, SQLContext).TotaleCommit()


   // ActorManagerRddToDataFrame()
    //NumeroEventPresente(pairRdd)
    //NumeroEventDivisiPerTypeActor()
    //NumeroEventDivisiPerTypeActorRepoSecondi()
    //MassimoPerSecondo()
    //MinimoPerSecondo()
    //MassimoPerAttore()
    //MinimoPerAttore()
    //MassimoPerRepo()
   // MinimoPerRepo()

  }
/*
  def rddToDataFrame() : Unit = {

    val dataStructGitHub = ScalaReflection.schemaFor[GitHubData].dataType.asInstanceOf[StructType]

    val gitHubDataRow = dataFrameToRDD().map(x=> {
      Row(x.id, x.`type`, x.actor, x.repo, x.payload, x.publicField, x.created_at, x.org)
    })

    val dataFrame = SQLContext.createDataFrame(gitHubDataRow, dataStructGitHub)

    dataFrame.show()

   // return dataFrame
  }

  def ActorManagerRddToDataFrame(): sql.DataFrame = {

    val schema_actor = ScalaReflection.schemaFor[Actor].dataType.asInstanceOf[StructType]

    /**  GETTING RDD */

    //getting actor rdd
    //val actor_rdd_from_JSON : Dataset[Row] = SQLContext.read.json("download\\2018-03-01-0.json").select("actor").rdd

    val actor_rdd : RDD[Actor] = rdd.map(x => x.actor)

    val actors_rows: RDD[Row] = actor_rdd.map(x => {
      Row(x.id, x.login, x.display_login, x.gravatar_id, x.url, x.avatar_url)
    })

    val df = SQLContext.createDataFrame(actors_rows, schema_actor)

    //df.show()

    return  df
  }

  def ActorManagerRddToPairRdd(): RDD[(String, Actor)] = {
    return dataFrame.map(x => (x.id, x.actor))
  }

  def ActorManagerRddToRdd() : RDD[Actor] = {
    return dataFrame.map(x => x.actor)
  }
*/
  def dataFrameToRDD() : RDD[GitHubData] = {
    //val data : Dataset[Row] = SQLContext.sql("select * from DataExtracted")//.limit(2000)
    val data = dataFrame
    import SQLContext.implicits._

    return data.as[GitHubData].rdd
  }
/*
  def ActorManagerRddToDataSet(dataFrameActor : sql.DataFrame) :  Dataset[Actor] = {
    dataset = dataFrameActor.as[Actor]
    return dataset
  }
*/
  //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
  def NumeroEventPresente(rddActor: RDD[(String, Actor)]) : Unit = {
    println("Numero Di event " + rddActor.count())

    println("Numero di Attori " + rddActor.groupByKey().count())

    println("Numero di Event per Autore  " )


    rddActor.groupByKey().map(f =>(f._1, f._2.size)).foreach(x => println(x._2))

  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def NumeroEventDivisiPerTypeActor() : Unit = {
    val idEventRDD = rdd.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRDD.foreach(x => println(x))
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo»;
  def NumeroEventDivisiPerTypeActorRepo() : Unit  ={
    val idEventRDD  = rdd.map(x=> ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    idEventRDD.foreach(x => println(x))
  }

  // Contare il numero di «event», divisi per «type», «actor», «repo» e secondo (il secondo è inteso come per tutto quel secondo. Quindi bisogna trasformare il timestamp in modo da avere solo il valore dei secondi, poi raggruppiamo solo su questo campo.);
  def NumeroEventDivisiPerTypeActorRepoSecondi() : RDD[((String,Actor,String,Int), Long)] =  {

    return rdd.map(x=> {
      ((x.`type`, x.actor, x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)
    }).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

  }

  //Trovare il massimoo numero di «event» per secondo

  def MassimoPerSecondo() : Unit = {
     val rdd= NumeroEventDivisiPerTypeActorRepoSecondi()

     val max = rdd.reduce((value1, value2)=> {
         if (value1._2 < value2._2) value2 else value1
       })

    println(max)
  }

  //Trovare il minimo numero di «event» per secondo
  def MinimoPerSecondo() : Unit = {
    val rdd= NumeroEventDivisiPerTypeActorRepoSecondi()

    val min = rdd.reduce((value1, value2)=> {
      if (value1._2 > value2._2) value2 else value1
    })
    println(min)
  }

  //Trovare il massimo numero di «event» per «actor»;
  def MassimoPerAttore() : Unit = {
    val idEventRDD = rdd.map(x => ( x.actor, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    val max = idEventRDD.reduce((value1, value2)=> {
      if (value1._2 > value2._2) value1 else value2
    })
  }

  //Trovare il minimo numero di «event» per «actor»;
  def MinimoPerAttore() : Unit = {
    val idEventRDD = rdd.map(x => ( x.actor, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    val min = idEventRDD.reduce((value1, value2)=> {
      if (value1._2 > value2._2) value2 else value1
    })
  }

  //Trovare il massimo numero di «event» per «repo»;
  def MassimoPerRepo() : Unit = {
    val idEventRDD = rdd.map(x => (x.repo, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRDD.reduce((value1, value2) => {
      if (value1._2 > value2._2) value1 else value2
    })
  }

  //Trovare il minimo numero di «event» per «repo»;
  def MinimoPerRepo() : Unit = {
    val idEventRdd = rdd.map(x => (x.repo, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1,value2) => {
      if(value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il massimo numero di «event» per secondo per «actor»;
  def MassimoPerSecondoAttore() : Unit = {
    val idEventRdd = rdd.map(x => ((x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1,value2) => {
      if(value1._2 > value2._2) value2 else value2
    })
  }
  //Trovare il minimo numero di «event» per secondo per «actor»;
  def MinimooPerSecondoAttore() : Unit = {
    val idEventRdd = rdd.map(x => ((x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if(value1._2 < value2._2) value1 else value2
    })
  }

  //Trovare il massimo numero di «event» per secondo per «repo»;
  def MassimoPerSecondoRepo() : Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1,value2) => {
      if(value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il minimo numero di «event» per secondo per «repo»;
  def MinimooPerSecondoRepo() : Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if(value1._2 < value2._2) value1 else value2
    })
  }
//Trovare il massimo numero di «event» per secondo per «repo» e «actor»;
  def MassimoPerSecondoRepoAttore() : Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1,value2) => {
      if(value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il minimo numero di «event» per secondo per «repo» e «actor»;
  def MinimooPerSecondoRepoAttore() : Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if(value1._2 < value2._2) value1 else value2
    })
  }

  //Contare il numero di «commit»;
  def TotaleCommit() : Unit = {
    //rdd.map(x => (x.payload.commits.map))
  }



  /*
    def createDataFrameStruct(): StructType = StructType(Seq(
      StructField("id", StringType, true),
      StructField("login" ,StringType , true),
      StructField("display_login", StringType, true),
      StructField("gravatar_id", StringType, true),
      StructField("url", StringType, true),
      StructField("avatar_url", StringType, true)
    ))



  */
}
