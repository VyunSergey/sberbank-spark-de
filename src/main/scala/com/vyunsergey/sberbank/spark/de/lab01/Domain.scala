package com.vyunsergey.sberbank.spark.de.lab01

import derevo.derive
import derevo.circe.snakeCodec
import derevo.cats.{order, eq => eqv}
import java.time.Instant

object Domain {
  final case class Input(userId: Int, itemId: Int, rating: Int, timestamp: Instant)
  object Input {
    def from(str: String, sep: String): Input = {
      val arr = str.split(sep).take(4)
      Input(
        userId = arr(0).toInt,
        itemId = arr(1).toInt,
        rating = arr(2).toInt,
        timestamp = Instant.ofEpochSecond(arr(3).toLong)
      )
    }
  }

  @derive(eqv, order)
  final case class ItemRatingKey(itemId: Int, rating: Int)
  object ItemRatingKey {
    def from(input: Input): ItemRatingKey =
      ItemRatingKey(
        itemId = input.itemId,
        rating = input.rating
      )
  }

  @derive(eqv, order)
  final case class ItemKey(itemId: Int)
  object ItemKey {
    def from(itemRatingKey: ItemRatingKey): ItemKey =
      ItemKey(
        itemId = itemRatingKey.itemId
      )
  }

  @derive(snakeCodec)
  final case class Solution(histFilm: List[Int], histAll: List[Int])
  object Solution {
    def filmFrom(ratings: Map[Int, Int]): Solution = {
      Solution(
        histFilm = ratings.toList.sortBy(_._1).map(_._2),
        histAll = Nil
      )
    }

  }
}
