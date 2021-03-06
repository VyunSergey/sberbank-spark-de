package com.vyunsergey.sberbank.spark.de.common.stream

import java.nio.file.{Files, Path}

import cats.effect.Sync

import fs2.{Stream, text}

trait StreamReader[F[_]] {
  def read(path: Path)(implicit S: Sync[F]): Stream[F, String] = {
    fs2.io.readInputStream[F](S.pure(Files.newInputStream(path)), 1024)
      .through(text.utf8Decode)
      .through(text.lines)
  }
}

object StreamReader {
  def apply[F[_]](implicit S: Sync[F]): StreamReader[F] = new StreamReader[F] {}
}
