package processes

import java.util.concurrent.Executors._

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import scalaz.concurrent.{ Strategy, Task }
import scalaz.stream.Process._
import scalaz.stream.io
import scalaz.stream.Process
import scalaz.stream.merge

class FilesSpec extends Specification {
  val logger = Logger.getLogger("files")
  val Sep = "\n"
  val parallelism = 6

  val in = "testdata/utf8.txt"
  val P = scalaz.stream.Process
  val E = newFixedThreadPool(parallelism, new NamedThreadFactory("text-processor"))

  "Parallel batch file processing. Ordered out" in {
    val out = "testdata/utf8-out0.txt"

    def invert(line: String): Process[Task, String] = P.eval(Task {
      logger.info(s"Read: $line")
      line.:\(Thread.currentThread().getName + ": ")((c, acc) ⇒ acc + c)
    }(E))

    val source = io.linesR(in)
      .chunk(parallelism)
      .flatMap(emitAll)
      .map(invert)

    val flow = merge.mergeN(parallelism)(source)(Strategy.Executor(E))
    ((flow.intersperse(Sep) |> scalaz.stream.text.utf8Encode) to io.fileChunkW(out)).run.run

    1 should be equalTo 1
  }

  "Parallel batch file processing. Unordered out" in {
    val out = "testdata/utf8-out1.txt"

    def invert(line: String): Task[String] = Task {
      logger.info(s"Read: $line")
      line.:\(Thread.currentThread().getName + ": ")((c, acc) ⇒ acc + c)
    }(E)

    val source = io.linesR(in)
      .chunk(parallelism)
      .flatMap(emitAll)
      .map(invert)
      .gather(parallelism)

    ((source.intersperse(Sep) |> scalaz.stream.text.utf8Encode) to io.fileChunkW(out)).run.run

    1 should be equalTo 1
  }
}