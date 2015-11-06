package crdt

import org.specs2.mutable.Specification
import com.rbmhtechnology.eventuate.{ Versioned, VectorTime, ConcurrentVersionsTree, ConcurrentVersions }

class EventuateConcurrentVersionsSpec extends Specification {

  def vectorTime1(t1: Long): VectorTime =
    VectorTime("replica1" -> t1)

  def vectorTime2(t1: Long, t2: Long): VectorTime =
    VectorTime("replica1" -> t1, "replica2" -> t2)

  def vectorTime3(t1: Long, t2: Long, t3: Long): VectorTime =
    VectorTime("replica1" -> t1, "replica2" -> t2, "replica3" -> t3)

  def vectorTime4(t1: Long, t2: Long, t3: Long, t4: Long): VectorTime =
    VectorTime("replica1" -> t1, "replica2" -> t2, "replica3" -> t3, "replica4" -> t4)

  "ConcurrentVersionsTree conflict" should {
    "be resolved with first version" in {
      val append: (String, String) ⇒ String =
        (a, b) ⇒ if (a == null) b else s"$a-$b"

      val cvt: ConcurrentVersions[String, String] = ConcurrentVersionsTree(append)

      cvt.update("A", vectorTime1(1))
        .update("B", vectorTime2(1, 1))
        .update("C", vectorTime3(1, 1, 1))
        .conflict === false

      cvt.update("D", vectorTime3(1, 1, 0))

      val winner = cvt.all(0).updateTimestamp
      //val winner = cvt.all(1).updateTimestamp
      cvt.conflict === true

      //Conflict between
      cvt.all(0).value === "A-B-C"
      cvt.all(1).value === "A-D"

      val merged = cvt.all.map(_.updateTimestamp).reduce(_ merge _) //vectorTime(1,1,1)

      println(s"Winner: $winner")
      println(s"All: ${cvt.all}")
      println(s"Merged: $merged")

      cvt.resolve(winner, merged)

      println(cvt.all)
      cvt.conflict === false

      cvt.all(0) === Versioned("A-B-C", merged)
      //cvt.all(0) === Versioned("A-D",vectorTime(1,1,1))
    }
  }
}