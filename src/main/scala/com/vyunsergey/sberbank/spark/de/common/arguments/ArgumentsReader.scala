package com.vyunsergey.sberbank.spark.de.common.arguments

import cats.Monad
import cats.implicits._
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ArgumentsReader(args: Seq[String]) extends ScallopConf(args) {
  val srcPath: ScallopOption[String] = opt[String](name = "src-path", validate = _.length > 0)
  val tgtPath: ScallopOption[String] = opt[String](name = "tgt-path", validate = _.length > 0)
  verify()
}

object ArgumentsReader {
  def apply[F[_]](args: Seq[String])(implicit M: Monad[F]): F[ArgumentsReader] =
    new ArgumentsReader(args).pure[F]
}
