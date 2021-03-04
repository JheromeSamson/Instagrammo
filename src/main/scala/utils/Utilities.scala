package utils

import java.io.{File, PrintWriter}

import scala.io.Source
import java.net.{HttpURLConnection, URL}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.io.FileOutputStream

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark
import org.apache.spark.SparkContext

import scala.sys.process._

class Utilities {

  val urlLink = "http://data.githubarchive.org/"

  val extensionFile = ".json.gz"

  val fileOutputStream ="C:\\Users\\jhero\\IdeaProjects\\BigData\\download\\"


  def fileDownloader(date: String) = {

    val url= new URL(urlLink + date + extensionFile)

    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.connect()

    if (connection.getResponseCode == 301){
      println("ERROR code: "+ connection.getResponseCode + println(connection.getResponseMessage) + "Redicting into " + connection.getHeaderField(2))

      val urlRedirect =  new URL(connection.getHeaderField(2))

      val urlConnection = urlRedirect.openConnection().asInstanceOf[HttpURLConnection]

      urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:25.0) Gecko/20100101 Firefox/25.0")

      val ungzip = new GZIPInputStream(urlConnection.getInputStream())
      val inputStream = ungzip
      val fos = new FileOutputStream("C:\\Users\\jhero\\IdeaProjects\\BigData\\download\\" + date + extensionFile)
      val gzos = new GZIPOutputStream( fos )
      val write    = new PrintWriter(gzos)

      for (line <- Source.fromInputStream(inputStream).getLines()) {
        println(line)
        write.write(line+"\n")
      }
      write.close()
      gzos.close()
      fos.close()
    }
    else
      url #> new File("vediamo") !!


  }

/*
  def fileDownloader(url: String = "http://data.githubarchive.org/2018-03-01-0.json.gz",
                     filename: String = "vediamo.gz") = {

    val outputFile = "C:\\Users\\jhero\\IdeaProjects\\BigData\\download\\download.gz"
    new URL(url) #> new File(filename)

    val in = new URL(url).openStream()
    Files.copy(in, Paths.get(outputFile), StandardCopyOption.REPLACE_EXISTING)
  }
*/
}
