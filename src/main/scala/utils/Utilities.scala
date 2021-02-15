package utils

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

import sys.process._
import java.net.URL
import java.io.File


import scala.sys.process._

class Utilities {

  def fileDownloader(url: String, filename: String) = {
    new URL(url) #> new File(filename) !!
  }

  def parsing(): Unit = {
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
