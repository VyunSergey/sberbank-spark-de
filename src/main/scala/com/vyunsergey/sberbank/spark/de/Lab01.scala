package com.vyunsergey.sberbank.spark.de

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.vyunsergey.sberbank.spark.de.common.arguments.ArgumentsReader
import com.vyunsergey.sberbank.spark.de.common.configuration.ConfigReader
import com.vyunsergey.sberbank.spark.de.common.stream.{StreamGrouper, StreamReader, StreamTransformer}
import com.vyunsergey.sberbank.spark.de.lab01.{ConfigPath, Domain}
import fs2.Stream

import scala.concurrent.duration._

object Lab01 extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    for {
      arguments <- ArgumentsReader[IO](args)
      _ <- println(s"Arguments: ${arguments.args.mkString("[", ", ", "]")}").pure[IO]
      config <- ConfigReader[IO].read(ConfigPath.path)
      _ <- println(s"Config: $config").pure[IO]
      srcPath <- arguments.srcPath.toOption.map(ConfigReader[IO].convertPath).getOrElse(config.srcPath.pure[IO])
      _ <- println(s"src-path: $srcPath").pure[IO]
      tgtPath <- arguments.tgtPath.toOption.map(ConfigReader[IO].convertPath).getOrElse(config.tgtPath.pure[IO])
      _ <- println(s"tgt-path: $tgtPath").pure[IO]
      stream = StreamReader[IO].read(srcPath)
      streamItemGroup = stream.filter(_.length > 0)
        .through(StreamTransformer[IO].transformAsync(a => Domain.Input.from(a, "\\s+").pure[IO], 16))
        .through(StreamGrouper[IO].groupMapReduce[Domain.Input, Domain.ItemRatingKey, Int]
          (Domain.ItemRatingKey.from)(_ => 1)(_ + _))
        .flatMap(a => Stream.emits(a.toSeq))
        .through(StreamGrouper[IO].groupMapReduce[(Domain.ItemRatingKey, Int), Domain.ItemKey, Map[Int, Int]]
          (a => Domain.ItemKey.from(a._1))(a => Map(a._1.rating -> a._2))(_ ++ _))
        .flatMap(a => Stream.emits(a.toSeq))
        .through(StreamTransformer[IO].transformAsync(a => (a._1, Domain.Solution.filmFrom(a._2)).pure[IO], 16))

      _ <- streamItemGroup
        .filter(_._1.itemId == 294)
        .foreach { in =>
        IO(println(s"Row: $in")) *> IO.sleep(1.second)
      }.compile.drain
    } yield ExitCode.Success
  }

}
