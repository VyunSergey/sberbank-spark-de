import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream

val lst = List
  .tabulate(10)(a => ((a + 1) % 3, 1))

val b = lst
  .groupMapReduce(_._1)(_._2)(_ + _)

val a: Unit = Stream.emits(lst)
  .debug(a => s"iterate: $a")
  .map(a => (a._1, 1))
  .debug(a => s"map: $a")
  .groupAdjacentBy(a => a._1)
  .debug(a => s"group: $a")
  .map(a => (a._1, a._2.map(_._2).foldLeft(0){
    case (acc, v) => acc + v
  }))
  .debug(a => s"map2: $a")
  .fold(Map.empty[Int, Int]){
    case (acc, (k, v)) =>
      acc.filterNot(_._1 == k) + acc.find(_._1 == k)
        .map(a => (a._1, a._2 + v))
        .getOrElse((k, v))
  }
  .debug(a => s"fold: $a")
  .foreach(a => IO(println(s"Row: $a")))
  .compile.drain.unsafeRunSync()

a


Map(1 -> 1, 2 -> 2) + (4, 4)
