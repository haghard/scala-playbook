package processes

import org.specs2.mutable.Specification

import scala.collection.mutable
import scalaz.stream.Process
import scalaz.stream._

class GeneralsSpec extends Specification {

  val P = Process

  "Writer" should {
    "write" in {
      val strOut = mutable.Buffer[String]()
      val intOut = mutable.Buffer[Int]()
      val naturals = P.emitAll(1 to 10)

      (for {
        o ← naturals.take(2)
        r ← (P.emitW("Incoming value: " + o) ++ P.emitO(o))
          .drainW(io.fillBuffer(strOut))
      } yield (r)).runLog.run

      (for {
        o ← naturals.take(5)
        r ← (P.emitW("Incoming value: " + o) ++ P.emitO(o))
          .drainO(io.fillBuffer(intOut))
      } yield (r)).runLog.run

      val a = mutable.Buffer[Int](1, 2, 3, 4, 5)
      val b = mutable.Buffer[String]("Incoming value: 1", "Incoming value: 2")

      intOut should be equalTo a
      strOut should be equalTo b
    }
  }
}
