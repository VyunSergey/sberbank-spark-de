package com.vyunsergey.sberbank.spark.de

import java.nio.file.Path

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._

import io.circe.syntax._

import com.vyunsergey.sberbank.spark.de.common.arguments.ArgumentsReader
import com.vyunsergey.sberbank.spark.de.common.configuration.ConfigReader
import com.vyunsergey.sberbank.spark.de.common.stream.{StreamGrouper, StreamReader, StreamTransformer, StreamWriter}
import com.vyunsergey.sberbank.spark.de.lab01.{ConfigPath, Domain}

import fs2.Stream
import fs2.concurrent.{NoneTerminatedQueue, Queue}

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
      stream <- program(294, -1, srcPath, tgtPath)
      _ <- stream.compile.drain
    } yield ExitCode.Success
  }

  def program(itemId: Int, allItemsId: Int,
              srcPath: Path, tgtPath: Path): IO[Stream[IO, Nothing]] = {
    for {
      stream <- readFileAsStream[Domain.Input](srcPath)(Domain.Input.from(_, "\\s+"))(16)
      itemId <- itemId.pure[IO]
      queue <- Queue.noneTerminated[IO, Domain.Input]
      streams <- splitStream[Domain.Input, Domain.Input](stream)(_.itemId == itemId)(_.copy(itemId = allItemsId))(identity)(queue)
      streamsGrp <- streams.map { in =>
        groupStream[Domain.Input, Domain.ItemRatingKey, Int](in)(Domain.ItemRatingKey.from)(_ => 1)(_ + _)
      }.map { in =>
        groupStream[(Domain.ItemRatingKey, Int), Domain.ItemKey, Map[Int, Int]](in)(a => Domain.ItemKey.from(a._1))(a => Map(a._1.rating -> a._2))(_ ++ _)
      }.pure[IO]
      streamSol <- foldMultipleStreams[(Domain.ItemKey, Map[Int, Int]), List[Domain.Solution]](streamsGrp, 2)(List.empty[Domain.Solution]) { case (acc, (k, m)) =>
        k match {
          case _ if k.itemId == itemId =>
            acc.headOption.getOrElse(Domain.Solution.from(m, Map.empty[Int, Int]))
              .copy(histFilm = m.toList.sortBy(_._1).map(_._2)) :: Nil
          case _ if k.itemId == allItemsId =>
            acc.headOption.getOrElse(Domain.Solution.from(Map.empty[Int, Int], m))
              .copy(histAll = m.toList.sortBy(_._1).map(_._2)) :: Nil
          case _ => acc
        }
      }.map(_.head)
        .debug(a =>s"Solution: $a")
        .pure[IO]
      result <- writeStreamInFile[Domain.Solution](streamSol)(tgtPath)(_.asJson.noSpaces)
    } yield result
  }

  def readFileAsStream[A](srcPath: Path)
                         (mapper: String => A)
                         (maxConcurrent: Int): IO[Stream[IO, A]] = {
    StreamReader[IO]
      .read(srcPath)
      .filter(_.length > 0)
      .through(StreamTransformer[IO].transformAsync(a => mapper(a).pure[IO], maxConcurrent))
      .pure[IO]
  }

  def splitStream[A, B](stream: Stream[IO, A])
                       (splitter: A => Boolean)
                       (mapper1: A => B)
                       (mapper2: A => B)
                       (queue: NoneTerminatedQueue[IO, A]): IO[Stream[IO, Stream[IO, B]]] = {
    Stream[IO, Stream[IO, B]](
      stream
        .evalTap(a => queue.enqueue1(Some(a)))
        .onFinalize(queue.enqueue1(None))
        .map(mapper1),
      queue
        .dequeue
        .filter(splitter)
        .map(mapper2)
    ).pure[IO]
  }

  def groupStream[A, K: Ordering, B](stream: Stream[IO, A])
                                    (selector: A => K)
                                    (mapper: A => B)
                                    (reducer: (B, B) => B): Stream[IO, (K, B)] = {
    stream
      .through(StreamGrouper[IO].groupMapReduce[A, K, B](selector)(mapper)(reducer))
      .flatMap(a => Stream.emits(a.toSeq))
  }

  def foldMultipleStreams[A, B](streams: Stream[IO, Stream[IO, A]], numStreams: Int)
                               (start: B)
                               (folder: (B, A) => B): Stream[IO, B] = {
    streams
      .parJoin(numStreams)
      .fold(start)(folder)
  }

  def writeStreamInFile[A](stream: Stream[IO, A])
                          (path: Path)
                          (converter: A => String): IO[Stream[IO, Nothing]] = {
    StreamWriter[IO]
      .write(stream.map(converter), path)
      .pure[IO]
  }

}
