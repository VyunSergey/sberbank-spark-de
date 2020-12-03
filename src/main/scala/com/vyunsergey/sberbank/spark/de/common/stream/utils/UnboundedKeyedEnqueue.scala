package com.vyunsergey.sberbank.spark.de.common.stream.utils

import cats.effect.Concurrent
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream
import fs2.concurrent.{NoneTerminatedQueue, Queue}

class UnboundedKeyedEnqueue[F[_], K, A](queues: Ref[F, Map[K, NoneTerminatedQueue[F, A]]])
                                       (implicit F: Concurrent[F]) extends KeyedEnqueue[F, K, A] {
  override def enqueue1(key: K, item: A): F[Option[(K, Stream[F, A])]] =
    withKey(key)(_.enqueue1(item.some))

  override val shutdownAll: F[Unit] =
    queues.get.flatMap(_.values.toList.traverse_(_.enqueue1(None)))

  private[this] def withKey(key: K)(
    use: NoneTerminatedQueue[F, A] => F[Unit]
  ): F[Option[(K, Stream[F, A])]] =
    queues.get.flatMap { qm =>
      qm.get(key)
        .fold {
          for { // No queue for key - create new one
            newQ <- Queue.noneTerminated[F, A]
            _ <- queues.update(x => x + (key -> newQ))
            _ <- use(newQ)
                } yield (key -> newQ.dequeue).some
        }(q => use(q).as(None))
    }
}
