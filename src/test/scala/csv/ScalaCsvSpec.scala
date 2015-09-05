package csv

import java.util.concurrent.Executors._
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import com.ambiata.origami.effect.{ SafeT, FinalizersException }
import com.nrinaudo.csv.RowReader
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.collection.mutable
import scala.concurrent.SyncVar
import scalaz.{ \/, -\/, \/- }
import scalaz.concurrent.Task

class ScalaCsvSpec extends Specification {
  import scalaz.stream.csv._
  import scalaz.stream.io._
  import scalaz.std.AllInstances._
  import com.ambiata.origami._, Origami._

  implicit val M = scalaz.Monoid[Int]

  val P = scalaz.stream.Process

  val logger = Logger.getLogger("scalaz-stream-csv")

  implicit val codec = scala.io.Codec.ISO8859

  case class RawLine(pipeId: String, description: String, name: String, metalMark: String, diameter: String, lenght: String)

  case class CvsLine(pipeId: String, description: String, name: String, metalMark: String, diameter: Double, lenght: Double)

  implicit val rowReader = RowReader(rec ⇒ RawLine(rec(0), rec(1), rec(2), rec(3), rec(4), rec(5)))

  val E = newFixedThreadPool(4, new NamedThreadFactory("csv-worker"))

  def LoggerSink = scalaz.stream.sink.lift[Task, CvsLine] { line ⇒ Task.delay(logger.debug(line.toString)) }

  def readBuffer = new FoldM[scalaz.Id.Id, String, Vector[CvsLine]] {
    type S = Vector[CvsLine]
    def start = Vector[CvsLine]()
    def fold = (state: S, current: String) ⇒ {
      val raw = current.split(";")
      if (raw.size == 6) {
        val lenght = raw(4).replace(',', '.')
        val diam = raw(5).replace(',', '.')
        state :+ CvsLine(raw(0), raw(1), raw(2), raw(3), lenght.toDouble, diam.toDouble)
      } else state
    }
    def end(state: S) = state
  }

  def Log4jSink: SinkM[scalaz.Id.Id, String] = new FoldM[scalaz.Id.Id, String, Unit] {
    override type S = org.apache.log4j.Logger
    override def fold = (state: S, line: String) ⇒ {
      state.debug(line)
      state
    }
    override val start: S = Logger.getLogger("logger")
    override def end(s: S): scalaz.Id.Id[Unit] = s.debug("Fold is competed")
  }

  def Log4jSinkIdLine: SinkM[scalaz.Id.Id, CvsLine] = new FoldM[scalaz.Id.Id, CvsLine, Unit] {
    val name = "origami-fold-logger"
    override type S = org.apache.log4j.Logger

    override def fold = (state: S, elem: CvsLine) ⇒ {
      state.debug(elem)
      state
    }
    override val start: scalaz.Id.Id[Logger] =
      Logger.getLogger(name)

    override def end(s: S): scalaz.Id.Id[Unit] =
      s.debug(s"$name is being competed")
  }

  def Log4jSinkTaskLine = new FoldM[SafeTTask, CvsLine, Unit] {
    val name = "origami-fold-logger"
    override type S = org.apache.log4j.Logger

    override def fold = (state: S, elem: CvsLine) ⇒ {
      state.debug(elem)
      state
    }

    override def end(s: S): SafeTTask[Unit] =
      SafeT.point[Task, Unit](s.debug(s"$name is being competed"))

    override def start: SafeTTask[Logger] =
      SafeT.point[Task, Logger](Logger.getLogger(name))
  }

  val list = List.iterate(0, 100)(_ + 1)
  val listD = List.iterate(0d, 100)(_ + 1.15d)

  def count0 = new FoldM.Fold[Int, Int] {
    type S = Int
    def start = 0
    def fold = (s: S, t: Int) ⇒ s + 1
    def end(s: S) = s
  }

  def plus0 = new FoldM.Fold[Int, Int] {
    type S = Int
    def start = 0
    def fold = (s: S, t: Int) ⇒ s + t
    def end(s: S) = s
  }

  def countConcurrent[T] = new FoldM[Task, Int, Int] {
    type S = Int
    def start = Task.delay(0)
    def fold = (s: S, t: Int) ⇒ Task { logger.info(s"State $s"); s + 1 }(E).run
    def end(s: S) = Task.delay(s)
  }

  "origami fold" should {
    "all variants" in {
      def countAndPlus: FoldM.Fold[Int, (Int, Int)] =
        count[Int] zip plus[Int]

      def maxAndMin: FoldM.Fold[Int, (Option[Int], Option[Int])] =
        minimum[Int] zip maximum[Int]

      def leftFoldCount = fromFoldLeft[Int, Int](0)((acc, c) ⇒ acc + 1)
      def foldM = fromMonoid[Int]
      def foldMapM = fromMonoidMap[Int, String](in ⇒ in.toString)

      def foldP: Fold[Int, List[Int]] =
        plus[Int] compose FoldId.list[Int]

      //FoldableM[Id, List[Int], Int].foldM(list)(count) === list.size
      //count.run(list) === list.size

      (foldM run list) == 4950
      (foldMapM run list) == list.foldLeft("")(_ + _.toString)
      (leftFoldCount run list) == 4950
      (countAndPlus run list) == (list.size, 4950)
      (maxAndMin run list) == (Some(0), Some(99))
      (foldP run list) == list.scan(0)(_ + _).tail
      (mean[Double] run listD) == 56.925000000000026d
    }

    "contramap" in {
      //FoldableM[scalaz.Id.Id, List[Double], Double]
      implicit val F = FoldableIsFoldableM[scalaz.Id.Id, List, Double]

      def foldContramap[T] =
        fromMonoid[Int].contramap[T](_ ⇒ 2)

      (foldContramap[Double] run listD) == listD.size * 2
    }

    "breakablePlus" in {
      implicit val F: FoldableM[scalaz.Id.Id, List[Int], Int] = FoldableIsFoldableM[scalaz.Id.Id, List, Int]

      def breakableWhenExceed(n: Int) =
        plus[Int].breakWhen(_ >= n)

      (breakableWhenExceed(5) runBreak (list)) == 6
      (breakableWhenExceed(6) runBreak (list)) == 6

      (breakableWhenExceed(7) runBreak (list)) == 10
      (breakableWhenExceed(10) runBreak (list)) == 10
    }

    "read file with origami" in {
      val Size = 127
      val sync = new SyncVar[Int]
      val source = scala.io.Source.fromFile("./cvs/metal2pipes.csv")

      def readFold: Fold[String, (Int, Vector[CvsLine])] =
        count[String] <*> (readBuffer observe Log4jSink)

      //SafeTIO
      def safeTask: SafeTTask[(Int, Vector[CvsLine])] =
        readFold.into[SafeTTask].run(source)

      val task: Task[(Throwable \/ (Int, Vector[CvsLine]), Option[FinalizersException])] =
        safeTask.`finally`(Task.delay { logger.info("File is being closed"); source.close() }).attemptRun

      Task.fork(task)(E).runAsync {
        case -\/(ex) ⇒ logger.info(s"Fold result ${ex.getMessage}")
        case \/-((\/-(res), None)) ⇒
          logger.info(s"Fold result ${res._1}")
          sync.put(res._1)
        case \/-((-\/(ex), None)) ⇒ logger.info(s"Fold error ${ex.getMessage}")
        case \/-((_, Some(ex)))   ⇒ logger.info(s"Fold finalizer error ${ex.getMessage}")
      }

      sync.get(5000) === Some(Size)
    }
  }

  "scalaz-stream-csv" should {
    val Path = "./cvs/metal2pipes.csv"

    "read with fold" in {
      val latch = new CountDownLatch(1)
      val buffer = mutable.Buffer.empty[CvsLine]
      val csvSource = rowsR[RawLine](Path, ';')

      val ReaderProcess = (csvSource map { raw ⇒
        val lenght = raw.lenght.replace(',', '.')
        val diam = raw.diameter.replace(',', '.')
        CvsLine(raw.pipeId, raw.description, raw.name, raw.metalMark, diam.toDouble, lenght.toDouble)
      } observe LoggerSink to fillBuffer(buffer))
        .foldMap(_ ⇒ 1)

      Task.fork(ReaderProcess.runLog)(E).runAsync {
        case \/-(r) ⇒
          logger.debug(s"Fold result: ${r.head}")
          latch.countDown
        case -\/(ex) ⇒
          logger.debug(ex.getMessage)
          latch.countDown
      }

      latch.await(5, TimeUnit.SECONDS) === true
    }

    "read with fold by origami" in {
      import com.ambiata.origami.stream.FoldableProcessM._

      val latch = new CountDownLatch(1)

      val source = rowsR[RawLine](Path, ';').map { raw ⇒
        val lenght = raw.lenght.replace(',', '.')
        val diam = raw.diameter.replace(',', '.')
        /*
        import scala.collection.JavaConversions.asScalaSet
        val blackList = Set("x-JISAutoDetect", "ISO-2022-CN")
        for {
          cSet ← asScalaSet(Charset.availableCharsets().keySet())
          if (!(blackList contains cSet))
        } {
          logger.debug(cSet + " -" + new String(raw.name.getBytes(), Charset.forName(cSet)))
        }
        */
        CvsLine(raw.pipeId, raw.description, raw.name, raw.metalMark, diam.toDouble, lenght.toDouble)
      }

      def foldCsvLines2: FoldM[SafeTTask, CvsLine, (Int, Double)] =
        ((count[CvsLine] observe Log4jSinkIdLine) <*> plusBy[CvsLine, Double](_.diameter)).into[SafeTTask]

      /*def foldCsvLines: FoldM[SafeTTask, CvsLine, Int] =
        (count[String].into[SafeTTask] observe Log4jSink2)
          .contramap[CvsLine](_.toString)
      */

      Task.fork((foldCsvLines2 run source).run)(E).runAsync {
        case \/-(r) ⇒
          logger.debug(s"Fold result: $r")
          latch.countDown
        case -\/(ex) ⇒
          logger.debug(ex.getMessage)
          latch.countDown
      }

      latch.await(5, TimeUnit.SECONDS) === true
    }
  }
}