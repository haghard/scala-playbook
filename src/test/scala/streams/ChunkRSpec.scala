package streams

import org.scalacheck.Prop._
import org.scalacheck.Properties

object ChunkRSpec extends Properties("chunkR") {

  property("read by tags") = forAll { source: Vector[Int] ⇒
    val chunks = Vector(10, 12, 67, 90)
    val tagsP = scalaz.stream.Process emitAll chunks

    val p = scalaz.stream.Process emitAll source
    val chunkedSource = streams.io.chunkR(p)

    val P = (for {
      size ← tagsP
      c ← chunkedSource.chunk(size)
    } yield (c)).onHalt(io.batcherHalter)

    val readSize = chunks.reduce(_ + _)

    val wasRead: IndexedSeq[Seq[Int]] = P.runLog.run

    if (readSize <= source.size) wasRead.flatten == source.splitAt(readSize)._1
    else wasRead.flatten == source
  }
}
