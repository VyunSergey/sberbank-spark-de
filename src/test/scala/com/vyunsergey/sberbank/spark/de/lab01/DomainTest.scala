package com.vyunsergey.sberbank.spark.de.lab01

import com.vyunsergey.sberbank.spark.de.lab01.Domain.Solution
import io.circe.syntax._
import io.circe.parser._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class DomainTest extends AnyFlatSpec with Matchers {
  "Circe Domain derivation" should "derive decoder & encoder" in {
    val solution = Solution(List(1, 2, 3, 4, 5), List(6, 7, 8, 9, 0))
    val solutionJson = """{"hist_film":[1,2,3,4,5],"hist_all":[6,7,8,9,0]}"""

    assert(solution.asJson.noSpaces === solutionJson)
    assert(decode[Solution](solutionJson) === Right(solution))
  }
}
