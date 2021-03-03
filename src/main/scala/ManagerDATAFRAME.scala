import org.apache.spark.sql
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, explode, hour, minute, second, size}

class ManagerDATAFRAME(val dataFrame : sql.DataFrame, val SQLContext: SQLContext) {

  //Trovare i singoli «actor»;
  def SingoliActor(): Unit =
    dataFrame.select("actor").distinct().show()

//Trovare i singoli «actor» commits ;
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
    val num = dataFrame.select("actor").distinct().count()
      println(num)
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

  //num commit
  def NumeroCommit() : Unit = {
    val num = dataFrame.select("commit").count()
    println(num)
  }
  // numero commit per attore
  def NumeroCommitPerActor() : Unit = {
    dataFrame.withColumn("commit",size(col("payload.commits"))).groupBy("actor","commit").count().show()
/*
    dataFrame.show()
    val payloadDF = dataFrame.select("payload.*").distinct()

    val commitsDF = payloadDF.select(explode(col("commits"))).select("col.*")

    import org.apache.spark.sql.functions.lit
    import org.apache.spark.sql.functions.when
    import org.apache.spark.sql.functions.size


    val payloadDF = dataFrame.withColumn("actor", lit(dataFrame.select("payload.*").select("commits[3]").count())).show()
    lit(dataFrame.select("payload.*").select("commits[3]"))


      dataFrame
      .withColumn("payload",lit(dataFrame.withColumn("commit",size(col("payload.commits"))).
      ))
      .groupBy("actor", "payload", "arrayCommit", "actor").count().show()


      lit(when(dataFrame.select("payload.*").select("commits[3]"))))
      .groupBy(col("actor"), ("payload"))
      select("payload.*").selectExpr("commits[3]")
    payloadDF.selectExpr("commits[3]").show()

    val commitsDF = payloadDF.select(explode(col("commits"))).select("col.*")

    val payload : String= dataFrame.select(explode(col("payload")))

    val commitsDF = dataFrame.select()payloadDF.select(explode(col("commits")))

    val c: String = dataFrame.withColumn("prova", col(dataFrame.select(explode(col("commits"))).count())))
*/
  }


  def MassimoCommitPerSecondi() : Unit = {
    import org.apache.spark.sql.functions.lit
    dataFrame.withColumn("seconds", second(col("created_at")))
      .select("payload.*").selectExpr("commits[3]")
      .groupBy("seconds").count().show()
  }

  //Contare il numero di actor, divisi per type e secondo
  def NumeroActorPerTypeSecond() : Unit = {
    val c= dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy( "`type`", "seconds","actor").count().show()
  }

  //Contare il numero di actor, divisi per repo type e secondo
  def NumeroActorPerRepoTypeSecond() : Unit = {
    val c= dataFrame.withColumn("seconds", second(col("created_at")))
      .groupBy( "repo","`type`", "seconds","actor").count().show()
  }

  //massimo numer


}
