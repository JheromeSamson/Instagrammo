import bean.{Actor, Commits, GitHubData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime

class ManagerRDD(val rdd : RDD[GitHubData], val SQLContext: SQLContext) {

  //Trovare i singoli «actor»;
  def SingoliActor(): Unit = {
    rdd.map(x => (x.actor, 1)).distinct().foreach(x => println(x))
  }

  //Trovare i singoli «Repo»;
  def SingloliRepo():  Unit = {
    rdd.map(x => x.repo).distinct().foreach(x => println(x))
  }

  //Trovare i vari tipi di evento «type»;
  def SingoliTipiType(): Unit = {
    rdd.map(x => x.`type`).distinct().foreach(x => println(x))
  }

  //Contare il numero di «actor»;
  def NumeroActor(): Unit = {
    val num = rdd.map(x => x.actor).distinct().count()
    println(num)
  }

  //Contare il numero di «repo»;
  def NumeroRepo(): Unit =  {
    val num = rdd.map(x => x.repo).distinct().count()
    println(num)
  }

  //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
  def NumeroEvent(): Unit = {

    val rddActor = rdd.map(x => (x.actor, x)).distinct()


    val c = rdd.map(x => (x.actor, x )).distinct()

    def seq = (lengthArray: Long, value: GitHubData) => lengthArray + 1
    def com = (x: Long, y: Long) => x + y

    val r = c.aggregateByKey(0L)(seq, com)

    r.foreach(x => println(x._2))
  }

  //Contare il numero di «event», divisi per  «actor»;
  def NumeroEventDivisiPerActor(): Unit = {
    val idEventRDD = rdd.map(x => (x.actor, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRDD.foreach(x => println(x))
  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def NumeroEventDivisiPerTypeActor(): Unit = {
    val idEventRDD = rdd.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRDD.foreach(x => println(x))
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo»;
  def NumeroEventDivisiPerTypeActorRepo(): Unit = {
    val idEventRDD = rdd.map(x => ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    idEventRDD.foreach(x => println(x))
  }

  // Contare il numero di «event», divisi per «type», «actor», «repo» e secondo (il secondo è inteso come per tutto quel secondo. Quindi bisogna trasformare il timestamp in modo da avere solo il valore dei secondi, poi raggruppiamo solo su questo campo.);
  def NumeroEventDivisiPerTypeActorRepoSecondi(): Unit = {
    rdd.map(x => {
      ((x.`type`, x.actor, x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)
    }).reduceByKey((contatore1, contatore2) => contatore1 + contatore2).foreach(x => println(x))
  }

  //Trovare il massimoo numero di «event» per secondo

  def NumeroMassimoEventPerSecondo(): Unit = {
    //val rdd = NumeroEventDivisiPerTypeActorRepoSecondi()

    val c = rdd.map(x => (new DateTime(x.created_at.getTime).getSecondOfMinute, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2).max()
    println(c)

/*
    val max = rdd.reduce((value1, value2) => {
      if (value1._2 < value2._2) value2 else value1
    })

    println(max)*/
  }

  //Trovare il minimo numero di «event» per secondo
  def NumeroMininoEventPerSecondo(): Unit = {

/*    val rdd = NumeroEventDivisiPerTypeActorRepoSecondi()

    val min = rdd.reduce((value1, value2) => {
      if (value1._2 > value2._2) value2 else value1
    })
    println(min)*/
  }

  //Trovare il massimo numero di «event» per «actor»;
  def MassimoPerAttore(): Unit = {
    val idEventRDD = rdd.map(x => (x.actor, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    val max = idEventRDD.reduce((value1, value2) => {
      if (value1._2 > value2._2) value1 else value2
    })
  }

  //Trovare il minimo numero di «event» per «actor»;
  def MinimoPerAttore(): Unit = {
    val idEventRDD = rdd.map(x => (x.actor, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    val min = idEventRDD.reduce((value1, value2) => {
      if (value1._2 > value2._2) value2 else value1
    })
  }

  //Trovare il massimo numero di «event» per «repo»;
  def MassimoPerRepo(): Unit = {
    val idEventRDD = rdd.map(x => (x.repo, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRDD.reduce((value1, value2) => {
      if (value1._2 > value2._2) value1 else value2
    })
  }

  //Trovare il minimo numero di «event» per «repo»;
  def MinimoPerRepo(): Unit = {
    val idEventRdd = rdd.map(x => (x.repo, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il massimo numero di «event» per secondo per «actor»;
  def MassimoPerSecondoAttore(): Unit = {
    val idEventRdd = rdd.map(x => ((x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il minimo numero di «event» per secondo per «actor»;
  def MinimooPerSecondoAttore(): Unit = {
    val idEventRdd = rdd.map(x => ((x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 < value2._2) value1 else value2
    })
  }

  //Trovare il massimo numero di «event» per secondo per «repo»;
  def MassimoPerSecondoRepo(): Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il minimo numero di «event» per secondo per «repo»;
  def MinimooPerSecondoRepo(): Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 < value2._2) value1 else value2
    })
  }

  //Trovare il massimo numero di «event» per secondo per «repo» e «actor»;
  def MassimoPerSecondoRepoAttore(): Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 > value2._2) value2 else value2
    })
  }

  //Trovare il minimo numero di «event» per secondo per «repo» e «actor»;
  def MinimooPerSecondoRepoAttore(): Unit = {
    val idEventRdd = rdd.map(x => ((x.repo, x.actor, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)

    idEventRdd.reduce((value1, value2) => {
      if (value1._2 < value2._2) value1 else value2
    })
  }

  //Contare il numero di «commit»
  def NumeroCommit(): Unit = {
    val c = rdd.map(x => (1, x.payload.commits))
    c.foreach(x => println(x))

    def seq = (lengthArray: Int, commitsArray: Array[Commits]) => if (commitsArray == null) lengthArray else lengthArray + commitsArray.length

    def com = (x: Int, y: Int) => x + y

    c.aggregateByKey(0)(seq, com).foreach(x => println(x))

  }

  //Contare il numero di «commit» per attore
  def NumeroCommitPerActor(): Unit = {
    val c = rdd.map(x => (x.actor, x.payload.commits))

    def seq = (lengthArray: Int, commitsArray: Array[Commits]) => if (commitsArray == null) lengthArray else lengthArray + commitsArray.length
    def com = (x: Int, y: Int) => x + y

    val r = c.aggregateByKey(0)(seq, com)

    r.foreach(x => println(x))
  }

  // Numero commit divisi per type e actor
  def NumeroCommitPerTypeActor(): Unit = {
    val c = rdd.map(x => ((x.`type`,x.actor), x.payload.commits))

    def seq = (lengthArray: Int, commitsArray: Array[Commits]) => if (commitsArray == null) lengthArray else lengthArray + commitsArray.length
    def com = (x: Int, y: Int) => x + y

    val r = c.aggregateByKey(0)(seq, com)

    r.foreach(x => println(x))
  }

  // Numero commit divisi per type e actor ????????event?????????
  def NumeroCommitPerTypeActorEvent(): Unit = {
  }

  // Numero commit divisi per type actor e Second
  def NumeroCommitPerTypeActorSecond(): Unit = {
    val c = rdd.map(x => ((x.`type`,x.actor,new DateTime(x.created_at.getTime).getSecondOfMinute), x.payload.commits))

    def seq = (lengthArray: Int, commitsArray: Array[Commits]) => if (commitsArray == null) lengthArray else lengthArray + commitsArray.length
    def com = (x: Int, y: Int) => x + y

    val r = c.aggregateByKey(0)(seq, com)

    r.foreach(x => println(x))
  }

  //MASSimo Numero Per Secondo di commit
  def MassimoCommitPerSecondo(): Unit = {
    val newrdd = rdd.map(x => (new DateTime(x.created_at.getTime).getSecondOfMinute, if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val massimo = newrdd.reduceByKey((x,y) => if(x > y) x else y)

    massimo.foreach(x => println(x))
  }

  //Minimo Numero di commit per secondo
  def MinimoCommitPerSecondo(): Unit = {
    val newrdd = rdd.map(x => (new DateTime(x.created_at.getTime).getSecondOfMinute, if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val minimo = newrdd.reduceByKey((x,y) => if(x > y) y else x)

    minimo.foreach(x => println(x))
  }

  //Massimo Numero di commit per actor
  def MassimoCommitPerActor(): Unit = {
    val newrdd = rdd.map(x => (x.actor, if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val massimo = newrdd.reduceByKey((x,y) => if(x > y) x else y)

    massimo.foreach(x => println(x))
  }

  //Numero Minimo commit per actor
  def MinimoCommitPerActor(): Unit = {
    val newrdd = rdd.map(x => (x.actor, if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val minimo = newrdd.reduceByKey((x,y) => if(x > y) y else x)

    minimo.foreach(x => println(x))
  }

  //Numero Minimo commit per repo
  def MassimoCommitPerRepo(): Unit = {
    val newrdd = rdd.map(x => (x.repo, if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val massimo = newrdd.reduceByKey((x,y) => if(x > y) x else y)

    massimo.foreach(x => println(x))
  }

  //Numero Minimo commit per repo
  def MinimoCommitPerRepo(): Unit = {
    val newrdd = rdd.map(x => (x.repo, if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val minimo = newrdd.reduceByKey((x,y) => if(x > y) y else x)

    minimo.foreach(x => println(x))
  }

  //Numero Minimo commit per secondo e actor
  def MassimoCommitPerSecondoActor(): Unit = {
    val newrdd = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor), if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val massimo = newrdd.reduceByKey((x,y) => if(x > y) x else y)

    massimo.foreach(x => println(x))
  }

  //Numero Minimo commit per repo
  def MinimoCommitPerSecondoActor(): Unit = {
    val newrdd = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.actor), if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val minimo = newrdd.reduceByKey((x,y) => if(x > y) y else x)

    minimo.foreach(x => println(x))
  }

  //Numero Minimo commit per secondo e actor
  def MassimoCommitPerSecondoRepo(): Unit = {
    val newrdd = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val massimo = newrdd.reduceByKey((x,y) => if(x > y) x else y)

    massimo.foreach(x => println(x))
  }

  //Numero Minimo commit per Secondo repo
  def MinimoCommitPerSecondoRepo(): Unit = {
    val newrdd = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo), if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val minimo = newrdd.reduceByKey((x,y) => if(x > y) y else x)

    minimo.foreach(x => println(x))
  }

  //Numero Minimo commit per secondo e Repo actor
  def MassimoCommitPerSecondoRepoActor(): Unit = {
    val newrdd = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor), if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val massimo = newrdd.reduceByKey((x,y) => if(x > y) x else y)

    massimo.foreach(x => println(x))
  }

  //Numero Minimo commit per secondo repo actor
  def MinimoCommitPerSecondoRepoActor(): Unit = {
    val newrdd = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor), if(x.payload.commits == null) 0 else x.payload.commits.length ))

    val minimo = newrdd.reduceByKey((x,y) => if(x > y) y else x)

    minimo.foreach(x => println(x))
  }

  def NumeroActorAttiviPerSecondo(): Unit = {
    //TODO
  }

  def NumeroActorPerTypeSecondo() : Unit ={

  }

  def NumeroActorPerRepoTypeSecondo() : Unit ={

  }

  def MassimoNumeroActorAttivoPerSecondo() : Unit = {
    //TODO
  }

  def MinimoNumeroActorAttivoPerSecondo() : Unit = {
    //TODO
  }

  def MassimoNumeroActorAttivoPerSecondoType() : Unit = {
    //TODO
  }

  def MinimoNumeroActorAttivoPerSecondoType() : Unit = {
    //TODO
  }

  def MassimoNumeroActorAttivoPerSecondoTypeRepo() : Unit = {
    //TODO
  }

  def MinimoNumeroActorAttivoPerSecondoTypeRepo() : Unit = {
    //TODO
  }
}