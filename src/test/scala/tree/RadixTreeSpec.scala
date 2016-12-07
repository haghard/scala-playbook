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

      val tree = RadixTree(words.zipWithIndex: _*)
      //RadixTree(
      //abacuses->0,abaft->1,abalone->2,abalones->3,abandon->4,
      //ball->5,bell->6,board->7,
      //zulu->8,zulus->9,zuni->10,zunis->11,zurich->12,zwieback->13,zwiebacks->14
      //)

      tree.filterPrefix("abal").values

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
