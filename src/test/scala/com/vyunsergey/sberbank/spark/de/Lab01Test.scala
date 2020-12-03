package com.vyunsergey.sberbank.spark.de

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._

import io.circe._
import io.circe.parser._

import com.vyunsergey.sberbank.spark.de.Lab01._
import com.vyunsergey.sberbank.spark.de.common.configuration.ConfigReader
import com.vyunsergey.sberbank.spark.de.lab01.Domain

import fs2.Stream
import fs2.concurrent.Queue

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class Lab01Test extends AsyncFreeSpec with Matchers {
  val srcPath: String = "src/test/resources/lab01/u.data"
  val tgtPath: String = "src/test/resources/lab01/solution.json"

  "readFileAsStream" - {
    "should read file as Stream" in {
      val program: IO[Domain.Input] =
        for {
          path <- ConfigReader[IO].convertPath(srcPath)
          stream <- readFileAsStream[Domain.Input](path)(Domain.Input.from(_, "\\s+"))(16)
          result <- stream
            .take(1)
            .debug(a => s"Parsed Row: $a")
            .compile
            .toList
            .map(_.head)
        } yield result

      program
        .map(a => a shouldBe Domain.Input.from("196\t242\t3\t881250949", "\\s+"))
        .unsafeRunSync()
    }
  }

  "splitStream" - {
    "should split stream into two parts" in {
      val program: IO[List[String]] =
        for {
          stream <- Stream(1, 2, 3, 4, 5).covary[IO].pure[IO]
          queue <- Queue.noneTerminated[IO, Int]
          streams <- splitStream[Int, String](stream)(_ % 2 == 1)(_.toString + "_1")(_.toString + "_2")(queue)
          result <- streams
            .parJoin(2)
            .debug(a => s"Row: $a")
            .compile
            .toList
        } yield result

      program
        .map { a =>
          a.filter(_.endsWith("_1")) shouldBe List("1_1", "2_1", "3_1", "4_1", "5_1")
          a.filter(_.endsWith("_2")) shouldBe List("1_2", "3_2", "5_2")
        }
        .unsafeRunSync()
    }
  }

  "groupStream" - {
    "should group stream into A => Map[K, B]" in {
      val program: IO[List[(Int, String)]] =
        for {
          stream <- Stream((1, "a"), (1, "b"), (1, "c"), (2, "d"), (3, "e"), (3, "f")).covary[IO].pure[IO]
          streamGroup <- groupStream[(Int, String), Int, String](stream)(_._1)(_._2)(_ ++ _).pure[IO]
          result <- streamGroup
            .debug(a => s"Row: $a")
            .compile
            .toList
        } yield result

      program
        .map { a =>
          a.filter(_._1 == 1) shouldBe List((1, "abc"))
          a.filter(_._1 == 2) shouldBe List((2, "d"))
          a.filter(_._1 == 3) shouldBe List((3, "ef"))
        }
        .unsafeRunSync()
    }
  }

  "foldMultipleStreams" - {
    "fold multiple streams into one stream" in {
      val program: IO[List[(Int, String)]] =
        for {
          stream1 <- Stream("a", "b", "c", "d", "e").covary[IO].pure[IO]
          stream2 <- Stream("1", "2", "3", "4", "5").covary[IO].pure[IO]
          streams <- Stream(stream1, stream2).covary[IO].pure[IO]
          streamFold <- foldMultipleStreams[String, List[(Int, String)]](streams, 2)(List.empty[(Int, String)]) {
            case (acc: List[(Int, String)], s: String) =>
              acc :+ (if (s.toCharArray.forall(_.isDigit)) (1, s) else (2, s))
          }.pure[IO]
          result <- streamFold
            .debug(a => s"Row: $a")
            .compile
            .toList
            .map(_.flatten)
        } yield result

      program
        .map { a =>
          a.filter(_._1 == 1).map(_._2) shouldBe List("1", "2", "3", "4", "5")
          a.filter(_._1 == 2).map(_._2) shouldBe List("a", "b", "c", "d", "e")
        }
        .unsafeRunSync()
    }
  }

  "writeStreamInFile" - {
    "should write stream in file" in {
      val hello = "Hello, world!"

      val writeProgram: IO[Unit] =
        for {
          path <- ConfigReader[IO].convertPath(tgtPath)
          stream <- Stream(hello).covary[IO].pure[IO]
            .map(_.debug(a => s"Write Row: $a"))
          write <- writeStreamInFile(stream)(path)(identity)
          result <- write
              .compile
              .drain
        } yield result

      writeProgram
        .map(_ shouldBe ())
        .unsafeRunSync()

      val readProgram: IO[List[String]] =
        for {
          path <- ConfigReader[IO].convertPath(tgtPath)
          stream <- readFileAsStream[String](path)(identity)(16)
          result <- stream
            .debug(a => s"Read Row: $a")
            .compile
            .toList
        } yield result

      readProgram
        .map(a => a shouldBe List(hello))
        .unsafeRunSync()
    }
  }

  "Tha Main program" - {
    "should correctly compute film statistics" in {
      val itemId = 294
      val allItemsId = -1

      val mainProgram: IO[Unit] =
        for {
          sourcePath <- ConfigReader[IO].convertPath(srcPath)
          targetPath <- ConfigReader[IO].convertPath(tgtPath)
          stream <- program(itemId, allItemsId, sourcePath, targetPath)
          result <- stream
            .compile
            .drain
        } yield result

      mainProgram
        .map(_ shouldBe ())
        .unsafeRunSync()

      val readResult: IO[String] =
        for {
          path <- ConfigReader[IO].convertPath(tgtPath)
          stream <- readFileAsStream[String](path)(identity)(16)
          result <- stream
            .debug(a => s"Read Row: $a")
            .compile
            .toList
            .map(_.head)
        } yield result

      val expectedResult: Either[Error, Domain.Solution] =
        parse("""{"hist_film":[47,73,168,151,46],"hist_all":[6110,11370,27145,34174,21201]}""")
          .flatMap(_.as[Domain.Solution])

      readResult
        .map(a => parse(a).flatMap(_.as[Domain.Solution]) shouldBe expectedResult)
        .unsafeRunSync()
    }
  }

}
