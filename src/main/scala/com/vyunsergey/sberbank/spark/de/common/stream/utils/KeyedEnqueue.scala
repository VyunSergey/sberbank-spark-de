package com.vyunsergey.sberbank.spark.de.common.stream.utils

import cats.effect.kernel.Ref
import cats.effect.std.Semaphore
import cats.effect.{Concurrent, Resource}
import cats.implicits._
import fs2.concurrent.NoneTerminatedQueue
import fs2.{Pipe, Stream}

trait KeyedEnqueue[F[_], K, A] {

  /** Enqueue a single item for a given key, possibly creating and returning a new substream for that key
   *
   * @return <ul><li> None if the item was published to an already-live substream </li>
   *         <li>Some if a new queue was created for this element. This can happen multiple times for the same
   *         key (for example, if the implementation automatically terminates old/quiet substreams).</li></ul>
   *         The returned stream may eventually terminate, but it won't be cancelled by this.
   */
  def enqueue1(key: K, item: A): F[Option[(K, Stream[F, A])]]

  /** Gracefully terminate all sub-streams we have emitted so far */
  def shutdownAll: F[Unit]
}

object KeyedEnqueue {
  def unbounded[F[_]: Concurrent, K, A]: Resource[F, KeyedEnqueue[F, K, A]] =
    Resource.liftF(Ref[F].of(Map.empty[K, NoneTerminatedQueue[F, A]])).flatMap {
      st =>
        Resource.make(
          (new UnboundedKeyedEnqueue[F, K, A](st): KeyedEnqueue[F, K, A])
            .pure[F])(_.shutdownAll)

    }

  def pipe[F[_]: Concurrent, K, A](ke: KeyedEnqueue[F, K, A])
                                  (selector: A => K): Pipe[F, A, (K, Stream[F, A])] = { in =>
    // Note this *must* be `++` specifically to allow for "input termination = output termination" behavior.
    // Using `onFinalize` will allow the finalizer to be rescoped to the output of this stream later, which
    // results in it not triggering because it's waiting for itself to terminate before it terminates itself
    in.evalMap(a => ke.enqueue1(selector(a), a)).unNone ++
      Stream.exec(ke.shutdownAll)
  }

  def itemBounded[F[_]: Concurrent, K, A](maxItems: Long): Resource[F, KeyedEnqueue[F, K, A]] =
    for {
      ke <- unbounded[F, K, A]
      limit <- Resource.liftF(Semaphore[F](maxItems))
    } yield new ItemBoundedKeyedEnqueue(ke, limit)
}




