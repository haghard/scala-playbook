package demo

import fastparse.WhitespaceApi
import fastparse.core.Parsed

object MathExpressions {

  object SimpleMathParser {

    import fastparse.all._

    val plus = P("+")
    val num: P[Int] = P(CharIn('0' to '9').rep.! /*rep(min = 1, max = 4)*/ ).!.map(_.toInt)
    val side: P[Int] = P("(" ~ expr ~ ")" | num).log()
    val expr: P[Int] = P(side ~ plus ~ side).map { case (l, r) ⇒ l + r }
  }

  object MathParser {
    val White = WhitespaceApi.Wrapper {
      import fastparse.all._
      NoTrace(" ".rep)
    }

    import White._
    import fastparse.noApi._

    def eval(tree: (Int, Seq[(String, Int)])): Int = {
      val (base, ops) = tree
      ops./:(base) {
        case (left, (op, right)) ⇒
          op match {
            case "+" ⇒ left + right
            case "-" ⇒ left - right
            case "*" ⇒ left * right
            case "/" ⇒ left / right
          }
      }
    }

    val number: P[Int] = P(CharIn('0' to '9').rep(1).!.map(_.toInt))
    val parens: P[Int] = P("(" ~/ addSub ~ ")")
    val factor: P[Int] = P(number | parens)

    val divMul: P[Int] = P(factor ~ (CharIn("*/").! ~/ factor).rep).map(eval)
    val addSub: P[Int] = P(divMul ~ (CharIn("+-").! ~/ divMul).rep).map(eval)
    val expr: P[Int] = P(" ".rep ~ addSub ~ " ".rep ~ End)
  }

  def main(args: Array[String]) = {
    val Parsed.Success(value, index) = SimpleMathParser.expr.parse("(7127+9)+(2+3)")
    println(s"Result: $value in position $index")

    val Parsed.Success(v, ind) = MathParser.expr.parse(" (1 + 1*2)+(3*4*5)/20")
    println(s"Result: $v in position $ind")
  }
}
