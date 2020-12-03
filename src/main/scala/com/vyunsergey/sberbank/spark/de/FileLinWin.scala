package com.vyunsergey.sberbank.spark.de

import java.io.File
import scala.io.Source

object FileLinWin {
  def main(args: Array[String]): Unit = {

    // "C:\Programs\sberbank-spark-de\src\main\resources\lab01\u.data"
    val pathNameWin = args(0)
    println(s"pathNameWin = $pathNameWin")
    // "C:/Programs/sberbank-spark-de/src/main/resources/lab01/u.data"
    val pathNameLin = args(1)
    println(s"pathNameLin = $pathNameLin")

    val pathW = new File(pathNameWin).toURI.getPath
    println(s"pathW = $pathW")
    val pathL = new File(pathNameLin).toURI.getPath
    println(s"pathL = $pathL")

    println("#======================================#")
    Source
      .fromFile(pathW)
      .getLines()
      .take(3)
      .foreach(println)
    println("#======================================#")
    Source
      .fromFile(pathL)
      .getLines()
      .take(3)
      .foreach(println)
    println("#======================================#")
  }
}
