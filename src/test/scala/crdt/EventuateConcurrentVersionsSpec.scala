package crdt

import org.specs2.mutable.Specification
import com.rbmhtechnology.eventuate.{VectorTime, ConcurrentVersionsTree, ConcurrentVersions}

class EventuateConcurrentVersionsSpec extends Specification {

  def vectorTime(t1: Int, t2: Int, t3: Int): VectorTime =
    VectorTime("replica1" -> t1, "replica2" -> t2, "replica3" -> t3)

  "ConcurrentVersionsTree conflict" should {
    "have resolved with first version" in {
      val append: (String, String) => String =
        (a, b) => if (a == null) b else s"$a-$b"

      val cvt: ConcurrentVersions[String, String] = ConcurrentVersionsTree(append)

      cvt.update("A", vectorTime(1,0,0))
        .update("B", vectorTime(1,1,0))
        .update("C", vectorTime(1,1,1))
        .conflict === false

      println(cvt.all)

      cvt.update("D", vectorTime(1,1,0))

      val branch = cvt.all.head.updateTimestamp
      //val branch = cvt.all.tail.head.updateTimestamp
      cvt.conflict === true

      cvt.all.map(_.value).head === "A-B-C"
      cvt.all.map(_.value).tail.head === "A-D"

      println(cvt.all)

      //val merged = branch.merge(cvt.all.head.updateTimestamp)
      val merged = branch.merge(cvt.all.tail.head.updateTimestamp)

      cvt.resolve(branch, merged)
      println(cvt.all)
      cvt.conflict === false

      cvt.all.map(_.value).head == "A-B-C"
      //cvt.all.map(_.value).head == "A-D"
    }
  }
}
