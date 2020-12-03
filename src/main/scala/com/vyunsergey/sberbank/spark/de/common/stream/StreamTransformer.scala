package com.vyunsergey.sberbank.spark.de.common.stream

import cats.effect.Concurrent
import fs2.Pipe

trait StreamTransformer[F[_]] {
  def transform[A, B](f: A => B): Pipe[F, A, B] = { in =>
    in.map(f)
  }

  def transformAsync[A, B](f: A => F[B], maxConcurrent: Int)
                          (implicit C: Concurrent[F]): Pipe[F, A, B] = { in =>
    in.mapAsync(maxConcurrent)(f)
  }
}

object StreamTransformer {
  def apply[F[_]]: StreamTransformer[F] = new StreamTransformer[F] {}
}
