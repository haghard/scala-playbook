package streams

import org.scalacheck.Prop._
import org.scalacheck.{ Arbitrary, Gen, Properties }

object ChunkRSpec extends Properties("chunkR") {

  def chunks = Gen.listOfN(10, Gen.chooseNum(10, 100))

  implicit def CSArbitrary: Arbitrary[List[Int]] = Arbitrary(chunks)

  property("read by chunk") = forAll { (source: Vector[Int], cs: List[Int]) ⇒
    val tagsP = scalaz.stream.Process emitAll cs

    val p = scalaz.stream.Process emitAll source
    val chunkedSource = streams.io.chunkR(p)

    val P = (for {
      size ← tagsP
      c ← chunkedSource.chunk(size)
    } yield c).onHalt(io.batcherHalter)

    val readSize = cs.sum

    val out: IndexedSeq[Seq[Int]] = P.runLog.run

    if (readSize <= source.size) out.flatten == source.splitAt(readSize)._1
    else out.flatten == source
  }
}
