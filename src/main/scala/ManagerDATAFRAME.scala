import org.apache.spark.sql
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, explode, hour, max, minute, second, size, sum, min, when, count}

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
    import org.apache.spark.sql.functions.lit
    import org.apache.spark.sql.functions.when
    import org.apache.spark.sql.functions.sum
    import org.apache.spark.sql.functions.size
    dataFrame
      .select("*")
      .withColumn("commitSize",size(col("payload.commits")))
      .groupBy("actor").agg(
      sum("commitSize").as("totSizeCommit")
    ).show()
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
  // Numero commit divisi per type e actor
  def NumeroCommitPerTypeActor() : Unit = {
    dataFrame
      .select("*")
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("`type`", "actor").agg(
      sum("commitSize").as("totSizeCommit")
    ).show()
  }

  // Numero commit divisi per type e actor ????????event?????????
  def NumeroCommitPerTypeActorEvent(): Unit = {
    //dataFrame.withColumn("commitSize", size(col("payload.commits"))).groupBy("`type`","actor")
  }

  // Numero commit divisi per type e Second
  def NumeroCommitPerTypeActorSecond(): Unit = {
    dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("`type`", "actor", "seconds" ).agg(
      sum("commitSize").as("totSizeCommit")
    ).show()
  }

  //MASSimo Numero Per Secondo di commit
  def MassimoCommitPerSecondo(): Unit ={

    import org.apache.spark.sql.functions.max
    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds" ).agg(
      sum("commitSize").as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(max("totSizeCommit").as("massimoCommit")).show()

  }

  //Minimo Numero di commit per secondo
  def MinimoCommitPerSecondo(): Unit ={

    import org.apache.spark.sql.functions.min
    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(min("totSizeCommit").as("massimoCommit")).show()

  }

  //Massimo Numero di commit per actor
  def MassimoCommitPerActor(): Unit = {

    import org.apache.spark.sql.functions.max
    val df = dataFrame
      .select("*")
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("actor" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(max("totSizeCommit").as("massimoCommit")).show()

  }

  //Numero Minimo commit per actor
  def MinimoCommitPerActor(): Unit = {

    import org.apache.spark.sql.functions.min
    val df = dataFrame
      .select("*")
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("actor" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(min("totSizeCommit").as("massimoCommit")).show()

  }


  //Numero Minimo commit per repo
  def MassimoCommitPerRepo(): Unit = {

    val df = dataFrame
      .select("*")
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("repo" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(max("totSizeCommit").as("massimoCommit")).show()

  }

  //Numero Minimo commit per repo
  def MinimoCommitPerRepo(): Unit = {

    import org.apache.spark.sql.functions.min
    val df = dataFrame
      .select("*")
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("repo" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(min("totSizeCommit").as("massimoCommit")).show()

  }

  //Numero Minimo commit per secondo e actor
  def MassimoCommitPerSecondoActor(): Unit = {

    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds", "actor" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(max("totSizeCommit").as("massimoCommit")).show()

  }

  //Numero Minimo commit per repo
  def MinimoCommitPerSecondoActor(): Unit = {

    import org.apache.spark.sql.functions.min
    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds", "actor" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(min("totSizeCommit").as("massimoCommit")).show()

  }


  //Numero Minimo commit per secondo e actor
  def MassimoCommitPerSecondoRepo(): Unit = {

    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds", "repo" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(max("totSizeCommit").as("massimoCommit")).show()

  }

  //Numero Minimo commit per Secondo repo
  def MinimoCommitPerSecondoRepo(): Unit = {

    import org.apache.spark.sql.functions.min
    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds", "repo" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(min("totSizeCommit").as("massimoCommit")).show()

  }


  //Numero Minimo commit per secondo e actor actor
  def MassimoCommitPerSecondoRepoActor(): Unit = {

    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds", "repo" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(max("totSizeCommit").as("massimoCommit")).show()

  }

  //Numero Minimo commit per secondo repo actor
  def MinimoCommitPerSecondoRepoActor(): Unit = {

    import org.apache.spark.sql.functions.min
    val df = dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .withColumn("commitSize", size(col("payload.commits")))
      .groupBy("seconds", "repo" ).agg(
      when(sum("commitSize") < 0, 0)
        .otherwise(sum("commitSize"))
        .as("totSizeCommit")
    )

    df.select("totSizeCommit").agg(min("totSizeCommit").as("massimoCommit")).show()

  }

  def NumeroActorAttiviPerSecondo(): Unit = {
    //TODO
  }

  def NumeroActorPerTypeSecondo() : Unit ={
    dataFrame
        .select("*")
      .withColumn("seconds", second(col("created_at")))
      .groupBy("type", "seconds" ).agg(
      count("actor")
    ).show()
  }

  def NumeroActorPerRepoTypeSecondo() : Unit ={
    dataFrame
      .select("*")
      .withColumn("seconds", second(col("created_at")))
      .groupBy("repo","type",  "seconds" ).agg(
      count("actor")
    ).show()
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
