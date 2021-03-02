import org.apache.spark.sql
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, explode, hour, minute, second}

class ManagerDATAFRAME(val dataFrame : sql.DataFrame, val SQLContext: SQLContext) {

  //Trovare i singoli «actor»;
  def SingoliActor(): Unit =
    dataFrame.select("actor").distinct().show()

//Trovare i singoli «actor»;
  def SingoliAuthorNelCommit(): Unit = {

    //dataFrame.show()
    val payloadDF = dataFrame.select("payload.*").distinct()

    val commitsDF = payloadDF.select(explode(col("commits"))).select("col.*")

    commitsDF.select("author").show()
  }

  //Trovare i singoli «actor»;
  def SingloliRepo():  Unit = {
    dataFrame.select("repo").distinct().show()
  }

  //Trovare i vari tipi di evento «type»;
  def SingoliTipiType(): Unit = {
    dataFrame.select("`type`").distinct().show()
  }

  //Contare il numero di «actor»;
  def NumeroActor(): Unit = {
    dataFrame.select("actor").distinct().count()
  }

  //Contare il numero di «repo»;
  def NumeroRepo(): Unit =  {
    val n = dataFrame.select("repo").distinct().count()
    println(n)
  }

  //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
  def NumeroEvent():  Unit =  {
    dataFrame.groupBy("actor").count().show()
  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def NumeroEventPerActor():  Unit = {
    dataFrame.groupBy("`type`", "actor").count().show()
  }

  // Contare il numero di «event», divisi per «type», «actor», «repo»;
  def NumeroEventPerTypeActorRepo() : Unit= {
    dataFrame.groupBy("`type`", "actor", "repo").count().show()
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo» e ora
  def NumeroEventPerTypeActorRepoSecondo() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
    .groupBy("`type`","actor", "repo", "seconds").count()
    dataFrame.show()
  }

  // Trovare il massimo numero di event per secondo
  def NumeroMassimoEventPerSecondo() :  Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy().max().show()
  }
  // Trovare il minimo numero di event per secondo
  def NumeroMininoEventPerSecondo() :  Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy().min().show()
  }
  // Trovare il massimo numero di event per Actor
  def NumeroMassimoEventPerActor(): Unit = {
    val df = dataFrame.withColumn("actor", col("actor") )
      .groupBy("actor").count()
    df.select("count").groupBy().max().show()
  }

  // Trovare il Minimo numero di event per Actor
  def NumeroMinimoEventPerActor(): Unit = {
    val df = dataFrame.withColumn("actor", col("actor") )
      .groupBy("actor").count()
    df.select("count").groupBy().min().show()
  }

  // Trovare il massimo numero di event per Repo
  def NumeroMassimoEventPerRepo(): Unit = {
    val df = dataFrame.withColumn("repo", col("repo") )
      .groupBy("repo").count()
    df.select("count").groupBy().max().show()
  }

  // Trovare il Minimo numero di event per Repo
  def NumeroMinimoEventPerRepo(): Unit = {
    val df = dataFrame.withColumn("repo", col("repo") )
      .groupBy("repo").count()
    df.select("count").groupBy().min().show()
  }

  // Trovare il Massimo numero di event per Attore
  def NumeroMassimoEventPerSecondoAttore() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy("actor").max().show()
  }

  // Trovare il Minimo numero di event per Attore
  def NumeroMinimoEventPerSecondoAttore() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy("actor").min().show()
  }

  // Trovare il Massimo numero di event per Attore
  def NumeroMassimoEventPerSecondoRepo() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy("repo").max().show()
  }

  // Trovare il Minimo numero di event per Attore
  def NumeroMinimoEventPerSecondoRepo() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy("repo").min().show()
  }

  // Trovare il Massimo numero di event per Repo Actor
  def NumeroMassimoEventPerSecondoRepoActor() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy("repo", "actor").max().show()
  }

  // Trovare il Minimo numero di event per Repo Actor
  def NumeroMinimoEventPerSecondoRepoActor() : Unit = {
    dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy("repo", "actor").min().show()
  }


}
