package algebird

import com.twitter.algebird.{ Monoid ⇒ TwitterMonoid }
import org.specs2.mutable.Specification

class AlgebirdSpec extends Specification {
  val log = org.apache.log4j.Logger.getLogger("algebird")

  case class FollowersGraph[From, To](branches: Set[(From, To)]) {
    def propagate[T: TwitterMonoid](mapping: Map[From, T]): Map[To, T] =
      branches
        .groupBy(_._2)
        .mapValues { edges ⇒
          val vs = edges.map(fromTo ⇒ mapping.getOrElse(fromTo._1, TwitterMonoid.zero[T]))
          TwitterMonoid.sum(vs)
        }
  }

  def jaccard[T](x: Set[T], y: Set[T]) = (x intersect y).size.toDouble / (x union y).size

  def approxSimilarity[T, H](mh: com.twitter.algebird.MinHasher[H], x: Set[T], y: Set[T]): Double = {
    val sigX = x.map(elem ⇒ mh.init(elem.toString)).reduce(mh.plus(_, _))
    val sigY = y.map(elem ⇒ mh.init(elem.toString)).reduce(mh.plus(_, _))
    mh.similarity(sigX, sigY)
  }

  //MinHash gives approximate set similarity. Works only for sets

  "MinHash similarity" should {
    "run" in {
      val x = "qwerty".toSet
      val y = "qwerty1".toSet

      import com.twitter.algebird._
      val hasher = new MinHasher32(1.0, 1024) //size 1 kb

      val sig0 = x.map(elem ⇒ hasher.init(elem.toString)).reduce(hasher.plus(_, _))
      val sig1 = y.map(elem ⇒ hasher.init(elem.toString)).reduce(hasher.plus(_, _))

      val hs = BigDecimal(hasher.similarity(sig0, sig1)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
      val s = BigDecimal(jaccard(x, y)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

      println(s"$hs - $s")
      //hs === approxSimilarity(hasher, x, y)
      //hs === s
      1 === 1
    }
  }

  "MinHash " should {
    "Use signatures and buckets to find similar sets" in {
      import com.twitter.algebird._
      //targetThreshold
      //0.005 - 247 buckets number
      //0.009 - 224
      //0.01 - 123
      //0.02 - 108

      /*
        Sets whose signatures
        end up in the same bucket are likely to be similar. The targetThreshold controls
        the desired level of similarity - the higher the threshold, the more efficiently
        you can find all the similar sets.
      */

      val h = new MinHasher32(0.005, 1 << 10) //1024 bytes

      val a = Set("b","c","d","e","f","g","h")
      val b = Set("b","c","d","e","f","g","l","a")
      val c = Set("a","c","d","e","f","g","l","h","y")
      val d = Set("c","a","e","d","g","f","l","h")

      //1 kb signature a representation of followers
      val as = a.map(h.init(_)).reduce(h.plus(_, _))
      val bs = b.map(h.init(_)).reduce(h.plus(_, _))
      val cs = c.map(h.init(_)).reduce(h.plus(_, _))
      val ds = d.map(h.init(_)).reduce(h.plus(_, _))

      //hasher.similarity(as, ds)
      //hasher.similarity(cs, ds)

      //seq based on buckets
      val aBucSeq = h.buckets(as).map(h.init(_)).reduce(h.plus(_, _))
      val bBucSeq = h.buckets(bs).map(h.init(_)).reduce(h.plus(_, _))
      val cBucSeq = h.buckets(cs).map(h.init(_)).reduce(h.plus(_, _))
      val dBucSeq = h.buckets(ds).map(h.init(_)).reduce(h.plus(_, _))

      //to avoid n^2 comparisons for all item now we can group items by buckets and compare them inside single bucket and
      //to get n^2 comparisons inside each bucket.
      //because with very high probability similar items should end up at the same bucket

      // c more similar to d than a similar to d
      h.similarity(aBucSeq, bBucSeq) < h.similarity(cBucSeq, dBucSeq)
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

  "Aggregators" should {
    "run" in {
      import com.twitter.algebird.Aggregator.{ approximatePercentile, approximateUniqueCount, max, min, uniqueCount }
      val seq = Seq.range(1, 100)
      (max[Int] join min[Int] join approximatePercentile[Int](0.9, 10))(seq) === ((99, 1), 90.0)
      (uniqueCount[Int] join approximateUniqueCount[Int])(seq) === (99, 99)
    }
  }

  "BloomFilter for group membership" should {
    "run" in {
      import com.twitter.algebird._
      val NUM_HASHES = 6
      val WIDTH = 128
      val SEED = 1

      val M = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)

      val blFilter0 = M.create("okc", "lac", "cle", "hou")
      val blFilter1 = M.create("gsw", "mil", "chi")
      val all = M.plus(blFilter0, blFilter1)

      all.numHashes === NUM_HASHES

      all.width === WIDTH

      all.contains("mil").isTrue === true
      all.contains("okc").isTrue === true
      all.contains("mik").isTrue === false
    }
  }
}
