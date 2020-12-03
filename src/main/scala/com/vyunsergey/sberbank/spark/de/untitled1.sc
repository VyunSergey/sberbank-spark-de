import java.io.File

import scala.io.Source

val pathNameWin = "C:\\Programs\\sberbank-spark-de\\src\\main\\resources\\lab01\\u.data"
val pathNameLin = "C:/Programs/sberbank-spark-de/src/main/resources/lab01/u.data"

val pathW = new File(pathNameWin).toURI.getPath
val pathL = new File(pathNameLin).toURI.getPath

Source
  .fromFile(pathW)
  .getLines()
  .take(3)
  .foreach(println)

Source
  .fromFile(pathL)
  .getLines()
  .take(3)
  .foreach(println)
