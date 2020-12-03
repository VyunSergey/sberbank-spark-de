package com.vyunsergey.sberbank.spark.de.common.stream

import cats.effect.Concurrent
import com.vyunsergey.sberbank.spark.de.common.stream.utils.KeyedEnqueue
import fs2.{Pipe, Stream}

trait StreamGrouper[F[_]] {
  /**
   * Grouping logic used to split a stream into sub-streams identified by a unique key.
   * Be aware that when working with an unbounded number of keys K, if streams never
   * terminate, there can potentially be unbounded memory usage.
   *
   * The emitted streams terminate when the input terminates or when the stream
   * returned by the pipe itself terminates. Termination is graceful and all input elements are emitted
   *
   * @param selector Function to retrieve grouping key from type A
   * @tparam A Elements in the stream
   * @tparam K A key used to group elements into subStreams
   * @return Streams grouped by a unique key identifier `K`.
   */
  def groupByUnbounded[A, K](selector: A => K)(
    implicit F: Concurrent[F]
  ): Pipe[F, A, (K, Stream[F, A])] = { in =>
    Stream
      .resource(KeyedEnqueue.unbounded[F, K, A])
      .flatMap { ke =>
        in.through(KeyedEnqueue.pipe(ke)(selector))
      }
  }

  /** Like `groupByUnbounded` but back pressures the stream when `maxItems` are inside */
  def groupBy[A, K](maxItems: Long)(selector: A => K)(
    implicit F: Concurrent[F]
  ): Pipe[F, A, (K, Stream[F, A])] = { in =>
    Stream
      .resource(KeyedEnqueue.itemBounded[F, K, A](maxItems))
      .flatMap { ke =>
        in.through(KeyedEnqueue.pipe(ke)(selector))
      }
  }

  def groupMapReduce[A, K, B](selector: A => K)(mapper: A => B)(reducer: (B, B) => B)
                             (implicit O: Ordering[K]): Pipe[F, A, Map[K, B]] = { in =>
    in.fold(Map.empty[K, B]) { case (acc, a) =>
      val k: K = selector(a)
      val b: B = mapper(a)
      val el: (K, B) = acc.find(_._1 == k)
        .map { case (_, bb) => (k, reducer(bb, b)) }
        .getOrElse((k, b))
      acc.filterNot(_._1 == k) + el
    }
  }

}

object StreamGrouper {
  def apply[F[_]]: StreamGrouper[F] = new StreamGrouper[F] {}
}
