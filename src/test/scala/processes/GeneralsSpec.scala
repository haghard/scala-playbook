package processes

import org.scalacheck.Gen
import org.specs2.mutable.Specification

import scala.collection.mutable
import scalaz.stream.Process
import scalaz.stream._
import Process._

class GeneralsSpec extends Specification {

  "Writer" should {
    "write" in {
      val strOut = mutable.Buffer[String]()
      val intOut = mutable.Buffer[Int]()
      val naturals = emitAll(1 to 10)

      (for {
        o ← naturals.take(2)
        r ← (emitW("Incoming value: " + o) ++ emitO(o))
          .drainW(io.fillBuffer(strOut))
      } yield (r)).runLog.run

      (for {
        o ← naturals.take(5)
        r ← (emitW("Incoming value: " + o) ++ emitO(o))
          .drainO(io.fillBuffer(intOut))
      } yield (r)).runLog.run

      val a = mutable.Buffer[Int](1, 2, 3, 4, 5)
      val b = mutable.Buffer[String]("Incoming value: 1", "Incoming value: 2")

      intOut should be equalTo a
      strOut should be equalTo b
    }
  }

  def mergeSorted[T: Ordering](left: List[T], right: List[T])(implicit ord: Ordering[T]): List[T] = {
    val source0 = emitAll(left)
    val source1 = emitAll(right)

    def choose(l: T, r: T): Tee[T, T, T] =
      if (ord.lt(l, r)) emit(l) ++ nextL(r)
      else emit(r) ++ nextR(l)

    def nextR(l: T): Tee[T, T, T] =
      tee.receiveROr[T, T, T](emit(l) ++ tee.passL)(r ⇒ choose(l, r))

    def nextL(r: T): Tee[T, T, T] =
      tee.receiveLOr[T, T, T](emit(r) ++ tee.passR)(l ⇒ choose(l, r))

    def init: Tee[T, T, T] =
      tee.receiveLOr[T, T, T](tee.passR)(nextR)

    (source0 tee source1)(init).toSource.runLog.run.toList
  }

  "MergeSort deterministically 2 sorted list" should {
    "run" in {
      val left = Gen.listOf(Gen.choose(0, 50)).map(_.sorted).sample.getOrElse(Nil)
      val right = Gen.listOf(Gen.choose(0, 50)).map(_.sorted).sample.getOrElse(Nil)
      println(left)
      println(right)

      val result = mergeSorted(left, right)
      println(result)
      result should be equalTo (left ::: right).sorted
    }
  }
}
