package com.vyunsergey.sberbank.spark.de.common.stream

import java.nio.file.{Files, Path}

import cats.effect.Sync
import cats.implicits._

import fs2.{Stream, text}

trait StreamWriter[F[_]] {
  def write(stream: Stream[F, String], path: Path)(implicit S: Sync[F]): Stream[F, Nothing] = {
    stream
      .through(text.utf8Encode)
      .through(fs2.io.writeOutputStream(Files.newOutputStream(path).pure[F]))
  }

}

object StreamWriter {
  def apply[F[_]](implicit S: Sync[F]): StreamWriter[F] = new StreamWriter[F] {}
}
