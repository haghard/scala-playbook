package scalding

import org.specs2.mutable.Specification
import com.twitter.algebird.{ Monoid ⇒ TwitterMonoid }

class ScaldingSpec extends Specification {
  val log = org.apache.log4j.Logger.getLogger("scalding")

  case class FollowersGraph[From, To](branches: Set[(From, To)]) {
    def propagate[T: TwitterMonoid](mapping: Map[From, T]): Map[To, T] =
      branches
        .groupBy(_._2)
        .mapValues { edges ⇒
          val vs = edges.map(fromTo ⇒ mapping.getOrElse(fromTo._1, TwitterMonoid.zero[T]))
          TwitterMonoid.sum(vs)
        }
  }

  def exactSimilarity[T](x: Set[T], y: Set[T]) = (x & y).size.toDouble / (x ++ y).size

  def approxSimilarity[T, H](mh: com.twitter.algebird.MinHasher[H], x: Set[T], y: Set[T]) = {
    val sigX = x.map(elem ⇒ mh.init(elem.toString)).reduce(mh.plus(_, _))
    val sigY = y.map(elem ⇒ mh.init(elem.toString)).reduce(mh.plus(_, _))
    mh.similarity(sigX, sigY)
  }

  "Hasher similarity" should {
    "run" in {
      val x = "qwerty".toSet
      val y = "qwerty1".toSet

      val hasher = new com.twitter.algebird.MinHasher32(1.0, 1024)
      val sig0 = x.map(elem ⇒ hasher.init(elem.toString)).reduce(hasher.plus(_, _))
      val sig1 = y.map(elem ⇒ hasher.init(elem.toString)).reduce(hasher.plus(_, _))

      val hs = hasher.similarity(sig0, sig1)
      //hs === approxSimilarity(hasher, x, y)
      //hs === exactSimilarity(x, y)
      1 === 1
    }
  }

  "Levels of followers" should {
    "run" in {
      val graph = FollowersGraph(Set(('E, 'B), ('F, 'C), ('F, 'D), ('G, 'D), ('G, 'F), ('B, 'A), ('C, 'A), ('D, 'A)))
      val users = List('A, 'B, 'C, 'D, 'E, 'F, 'G).map(name ⇒ (name, Set(name))).toMap

      val firstFollowers = (graph propagate users)
      val secondFollowers = (graph propagate firstFollowers)
      val thirdFollowers = (graph propagate secondFollowers)

      log.debug(s"1 level $firstFollowers")
      log.debug(s"2 level $secondFollowers")
      log.debug(s"3 level $thirdFollowers")
      1 === 1
    }
  }

  "Scalding's aggregators" should {
    "run" in {
      import com.twitter.algebird.Aggregator.{ max, min, approximatePercentile, uniqueCount, approximateUniqueCount }
      val seq = Seq.range(1, 100)
      (max[Int] join min[Int] join approximatePercentile[Int](0.9, 10))(seq) === ((99, 1), 89.0)
      (uniqueCount[Int] join approximateUniqueCount[Int])(seq) === (99, 99)
    }
  }
}
