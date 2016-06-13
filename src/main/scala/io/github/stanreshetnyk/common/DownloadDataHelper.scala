package io.github.stanreshetnyk.common


import org.apache.commons.io.FileUtils

import scala.util.{Failure, Success, Try}
import sys.process._
import java.net.URL
import java.io.File

/**
  * Created by Stan Reshetnyk on 09.06.16.
  */
object DownloadDataHelper {

  def download(baseUrl: String, storeLocation: String, files: List[String]) = {
    // download each file
    files.foreach(file => Try({
      val from = baseUrl + file
      val to = storeLocation + file
      val toFile = new File(to)

      // create all missing subdirectories
      toFile.getParentFile().mkdirs()

      // download url content to file
      new URL(from) #> toFile !!

      if (file.endsWith(".zip")) {
        // TODO handle
      }

      println("Downloaded " + file + " of size: " + FileUtils.byteCountToDisplaySize(toFile.length()))
    }) match {
      case Success(file) =>

      case Failure(e) =>
        println("Error downloading " + file + ", error:" + e)
    })
  }
}
