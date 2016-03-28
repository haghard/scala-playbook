package demo

import fastparse.core.Parsed

object SumExpression {

  object Parser {

    import fastparse.all._

    val plus = P("+")
    val num: P[Int] = P(CharIn('0' to '9').rep.! /*rep(min = 1, max = 4)*/ ).!.map(_.toInt)
    val side: P[Int] = P("(" ~ expr ~ ")" | num)
    val expr: P[Int] = P(side ~ plus ~ side).map { case (l, r) ⇒ l + r }
  }

  def main(args: Array[String]) = {
    val Parsed.Success(value, index) = Parser.expr.parse("(7127+9)+(2+3)")
    println(s"Result: $value in position $index")
  }
}
