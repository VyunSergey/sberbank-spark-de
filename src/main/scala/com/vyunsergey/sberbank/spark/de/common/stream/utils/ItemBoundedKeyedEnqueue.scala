package com.vyunsergey.sberbank.spark.de.common.stream.utils

import cats.Monad
import cats.effect.std.Semaphore
import cats.implicits._
import fs2.Stream

class ItemBoundedKeyedEnqueue[F[_]: Monad, K, A](
                                                  ke: KeyedEnqueue[F, K, A],
                                                  limit: Semaphore[F]
                                                ) extends KeyedEnqueue[F, K, A] {
  override def enqueue1(key: K, item: A): F[Option[(K, Stream[F, A])]] =
    limit.acquire >> ke
      .enqueue1(key, item)
      .map(_.map {
        case (key, stream) =>
          // We only need to attach the "release" behavior to a given stream once because each stream is emitted once, and then reused
          key -> stream.chunks
            .evalTap(c => limit.releaseN(c.size.toLong))
            .flatMap(Stream.chunk)
      })

  override val shutdownAll: F[Unit] = ke.shutdownAll
}
