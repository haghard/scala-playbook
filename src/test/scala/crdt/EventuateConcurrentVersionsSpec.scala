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

  val state: (String, String) ⇒ String =
    (a, b) ⇒
      if (a == null) b else s"$a-$b"

  "ConcurrentVersionsTree conflict" should {
    "be resolved" in {
      val cvt: ConcurrentVersions[String, String] = ConcurrentVersionsTree(state).withOwner("replica1")

      cvt.update("A", vectorTime1(1))
        .update("B", vectorTime2(1, 1))
        .update("C", vectorTime3(1, 1, 1))
        .conflict === false

      cvt.update("D", vectorTime3(1, 1, 0))

      val pickedValue = cvt.all(0).vectorTimestamp
      //val pickedValue = cvt.all(1).updateTimestamp
      cvt.conflict === true

      //Conflict between
      cvt.all(0).value === "A-B-C"
      cvt.all(1).value === "A-D"

      val merged = cvt.all.map(_.vectorTimestamp).reduce(_ merge _) //vectorTime(1,1,1)

      println(s"Picked: $pickedValue")
      println(s"All: ${cvt.all}")
      println(s"Merged: $merged")

      (cvt resolve (pickedValue, merged))

      println(cvt.all)
      cvt.conflict === false

      cvt.all(0) === Versioned("A-B-C", merged)
      //cvt.all(0) === Versioned("A-D",vectorTime(1,1,1))
    }
  }

  /*
  "ORSet" should {
    "merge 3" in {
      val state = com.rbmhtechnology.eventuate.crdt.ORSet[String]()

      //localTime = s.versionedEntries./:(localTime)(_ merge _.vectorTimestamp).increment(product)
      val add1 = state.add("a", vectorTime1(1))

      //all.map(_.vectorTimestamp).reduce(_ merge _), all.map(_.systemTimestamp).max

      add1.versionedEntries.map(_.vectorTimestamp)
        //scan(false)(_.vectorTimestamp.conc(_))

        //.add("b", vectorTime1(1))
        //.add("c", vectorTime1(1))


      val res = add1.value

      res.size === 3
      res === Set("a","b","c")

      /*
      LoggerE.info(s"Local history on $num: $localTime")
      LoggerE.info(s"History of merge on $num: ${set.versionedEntries}")
      LoggerE.info(s"Purchases on $num: ${set.versionedEntries.map(_.value)}")
      set.value
      */

    }
  }*/
}