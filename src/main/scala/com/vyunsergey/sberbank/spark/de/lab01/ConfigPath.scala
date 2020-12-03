package com.vyunsergey.sberbank.spark.de.lab01

import java.io.File
import java.nio.file.{Path, Paths}

object ConfigPath {
  val path: Path = Paths.get(new File(getClass.getResource("/lab01/application.conf").toURI.getPath).toURI)
}
