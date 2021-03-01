import org.apache.spark.sql
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, explode,hour,minute,second}

class ManagerDATAFRAME(val dataFrame : sql.DataFrame, val SQLContext: SQLContext) {

  //Trovare i singoli «actor»;
  def SingoliActor(): sql.DataFrame =
    dataFrame.select("actor").distinct()

//Trovare i singoli «actor»;
  def SingoliAuthorNelCommit(): sql.DataFrame = {

    //dataFrame.show()
    val payloadDF = dataFrame.select("payload.*").distinct()

    val commitsDF = payloadDF.select(explode(col("commits"))).select("col.*")

    commitsDF.select("author")
  }

  //Trovare i singoli «actor»;
  def SingloliRepo():  sql.DataFrame = {
    dataFrame.select("repo").distinct()
  }

  //Trovare i vari tipi di evento «type»;
  def SingoliTipiType(): sql.DataFrame = {
    dataFrame.select("`type`").distinct()
  }

  //Contare il numero di «actor»;
  def NumeroActor(): Long = {
    dataFrame.select("actor").distinct().count()
  }

  //Contare il numero di «repo»;
  def NumeroRepo(): Long =  {
    dataFrame.select("repo").distinct().count()
  }

  //Contare il numero di «event» (con «event» intendo l’oggeto principale del json parsato) per ogni «actor»;
  def NumeroEvent():  sql.DataFrame =  {
    dataFrame.groupBy("actor").count()
  }

  //Contare il numero di «event», divisi per «type» e «actor»;
  def NumeroEventPerActor():  sql.DataFrame = {
    dataFrame.groupBy("`type`", "actor").count()
  }

  // Contare il numero di «event», divisi per «type», «actor», «repo»;
  def NumeroEventPerTypeActorRepo() : sql.DataFrame = {
    dataFrame.groupBy("`type`", "actor", "repo").count()
  }

  //Contare il numero di «event», divisi per «type», «actor», «repo» e ora
  def NumeroEventPerTypeActorRepoOra() : sql.DataFrame = {
    dataFrame.withColumn("seconds", second(col("created_at")))
    .groupBy("`type`","actor", "repo", "seconds").count()
  }



}
