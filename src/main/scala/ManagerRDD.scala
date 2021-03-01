import bean.{Actor, GitHubData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime

class ManagerRDD(val rdd : RDD[GitHubData], val SQLContext: SQLContext) {


  //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
  def NumeroEventPresente(rddActor: RDD[(String, Actor)]) : Unit = {


    println("Numero di Attori " + rddActor.groupByKey().count())

    println("Numero di Event per Autore  " )

    rddActor.groupByKey().map(f =>(f._1, f._2.size)).foreach(x => println(x._2))

  }

  def NumeroTotaleActor(rddActor: RDD[(String, Actor)]): Unit = {
    rddActor.count()
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



}
