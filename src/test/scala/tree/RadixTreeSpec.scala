package tree

import org.specs2.mutable.Specification

class RadixTreeSpec extends Specification {
  import com.rklaehn.radixtree._
  import cats.implicits._

  "RadixTree" should {

    "filterPrefix" in {
      val words = Array(
        "abacuses", "abaft", "abalone", "abalones", "abandon",
        "ball", "bell", "board",
        "zulu", "zulus", "zuni", "zunis", "zurich", "zwieback", "zwiebacks")
      //scala.io.Source.fromURL("http://www-01.sil.org/linguistics/wordlists/english/wordlist/wordsEn.txt").getLines.toArray
      val pairs = words.map(x ⇒ x -> x)
      val searchTree = RadixTree(pairs: _*)

      println(searchTree.show)

      //print all english words starting with b and z

      val b = Seq("ball", "bell", "board")
      searchTree.filterPrefix("b").values === b
      searchTree.filterPrefix("b").keys === b

      val z = Seq("zulu", "zulus", "zuni", "zunis", "zurich", "zwieback", "zwiebacks")
      searchTree.filterPrefix("z").values === z
      searchTree.filterPrefix("z").keys === z
    }

    "word count" in {
      import algebra.ring.AdditiveMonoid
      import algebra.instances.all._

      //val text = scala.io.Source.fromURL("http://classics.mit.edu/Homer/odyssey.mb.txt").getLines
      val text = Array("abcbcd efghjdfsf abcbcd  efghjdfsf abcbcd efghjdfsf", "abcbcd efghjdfsf abcbcd  efghjdfsf abcbcd efghjdfsf")
      val words = text.flatMap(_.split("\\s+")).filterNot(_.isEmpty)
      val M = AdditiveMonoid[RadixTree[String, Int]]

      val countTree = (words.map(x ⇒ RadixTree(x -> 1)) reduce M.plus)

      //println(countTree.entries)
      countTree.keys === List("abcbcd", "efghjdfsf")
      countTree.values === List(6, 6)

      countTree("abcbcd") === 6
      countTree("efghjdfsf") === 6
    }
  }
}
