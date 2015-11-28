package processes

import org.scalacheck.Gen
import org.apache.log4j.Logger
import scalaz.stream.Cause.End
import scalaz.stream.ReceiveY._
import scalaz.{ \/-, -\/, \/ }
import scalaz.stream.Process._
import scala.concurrent.SyncVar
import scala.collection.IndexedSeq
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors._
import org.specs2.mutable.Specification
import mongo.MongoProgram.NamedThreadFactory
import scala.concurrent.forkjoin.ThreadLocalRandom

import scala.concurrent.duration.{ FiniteDuration, Duration }

import scalaz.stream._
import scalaz.concurrent.{ Strategy, Task }

class ScalazProcessRepertoires extends Specification {
  val P = scalaz.stream.Process
  val logger = Logger.getLogger("proc-binding")

  "Interleave results" should {

    "Non-deterministic interleave 2 streams through merge/either" in {
      implicit val strategy =
        Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("io-worker")))

      def ioTask(latency: Int): Task[Int] = Task.async(blockingIO(latency))

      def blockingIO(n: Int): (Throwable \/ Int ⇒ Unit) ⇒ Unit =
        callback ⇒ try {
          val result = (math.random * 100).toInt
          logger.info(s"Start: $result")
          Thread.sleep(n) // simulate blocking call
          logger.info(s"Done: $result")
          callback(\/-(result))
        } catch {
          case t: Throwable ⇒ callback(-\/(t))
        }

      def externalSource(sleepTime: Int) = Process.repeatEval(ioTask(sleepTime))

      val l = externalSource(800)
      val r = externalSource(900)

      //arbitrary nondeterminism
      (l merge r)
        .map(_.toString).take(10)
        .to(io.stdOutLines)
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run

      (l either r)
        .map(_.toString).take(10)
        .to(io.stdOutLines)
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process has been completed"))) }
        .run.run

      true should be equalTo true
    }

    "Non-deterministic interleave merge results with mergeN" in {
      val range = 0 until 50
      val ioExecutor = newFixedThreadPool(4, new NamedThreadFactory("io-executor"))
      val fanOutS = Strategy.Executor(newFixedThreadPool(1, new NamedThreadFactory("fan-out")))

      val sum = range.sum
      val sync = new SyncVar[Throwable \/ IndexedSeq[Int]]

      def resource(url: Int): Process[Task, Int] = P.eval(Task {
        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200)) // simulate blocking call
        logger.info(s"Get $url")
        url
      }(ioExecutor))

      val source: Process[Task, Process[Task, Int]] =
        emitAll(range) |> process1.lift(resource)

      import scalaz.std.AllInstances._
      merge.mergeN(source)(fanOutS).foldMonoid.runLog.runAsync(sync.put)
      sync.get should be equalTo \/-(IndexedSeq(sum))
    }
  }

  "Parallel map-reduce through queue" should {
    "word count with monoid" in {
      import scalaz._
      import Scalaz._
      val PLogger = Logger.getLogger("map-reduce")

      //identity op(a, zero) == a
      //associativity op(op(a,b),c) == op(a, op(b,c))

      val mSize = Runtime.getRuntime.availableProcessors / 2
      val q = async.boundedQueue[String](mSize * mSize)
      implicit val S =
        Strategy.Executor(newFixedThreadPool(mSize, new NamedThreadFactory("map-reduce")))

      val inWords: Process[Task, String] =
        P.emitAll(Seq("A A", "A", "B", "C", "B", "C", "C", "B", "B", "C", "B", "C", "C A", "B", "B", "C", "B", "C", "C", "B", "B A"))

      val qWriter: Process[Task, Unit] =
        (inWords to q.enqueue)
          .onComplete(Process.eval_ { PLogger.info("All input was scheduled. Close queue"); q.close })

      val mappers: Process[Task, Process[Task, Map[String, Int]]] =
        P.range(0, mSize) map { i ⇒
          PLogger.info(s"Start mapper process №$i")
          q.dequeue.map { line ⇒
            val m = line.split(" ").toList.foldMap(i ⇒ Map(i -> 1))
            PLogger.info(s"Mapper input: $line  out: $m")
            m
          }
        }

      val out: IndexedSeq[Map[String, Int]] =
        (qWriter.drain merge scalaz.stream.merge.mergeN(mSize)(mappers)(S))
          .foldMonoid[Map[String, Int]]
          .runLog.run

      out.size === 1
      out.head("B") === 10
      out.head("C") === 9
      out.head("A") === 5
    }
  }

  /*
   * The resulting streams can be pulled independently on different rates,
   * though they will propagate back pressure if one of them is running too far ahead of the other
   */
  def broadcastN[T](n: Int = 2, source: Process[Task, T], limit: Int = 10)(implicit S: scalaz.concurrent.Strategy): Process[Task, Seq[Process[Task, T]]] = {
    val queues = (0 until n) map (_ ⇒ async.boundedQueue[T](limit)(S))
    val broadcast = queues./:(source)((src, q) ⇒ (src observe q.enqueue))
      .onComplete {
        Process.eval {
          logger.info("All input was scheduled")
          Task.gatherUnordered(queues.map(_.close))
        }
      }
    (broadcast.drain merge P.emit(queues.map(_.dequeue)))(S)
  }

  "Broadcast single process for N output processes which gets slower with time" should {
    "left behind maximum on queue size and slow down publisher" in {
      implicit val E = newFixedThreadPool(4, new NamedThreadFactory("broadcast"))
      implicit val S = Strategy.Executor(E)

      val source = P.emitAll(1 to 35)
      val sync = new SyncVar[Throwable \/ Unit]()

      val p = for {
        outs ← broadcastN(3, source)

        consumer0 = outs(0).zip(P.repeatEval(Task {
          val delayPerMsg = 500
          Thread.sleep(delayPerMsg)
          delayPerMsg
        })).map(r ⇒ logger.info(s"${r._2} consumer_0 ${r._1}"))

        //degrade on every iteration by 20
        consumer1 = outs(1).zip(P.suspend {
          @volatile var latency0 = 0
          P.repeatEval(Task {
            val init = 500
            latency0 += 20
            val delay = init + latency0
            Thread.sleep(delay)
            delay
          })
        }).map(r ⇒ logger.info(s"${r._2} consumer_1 ${r._1}"))

        //degrade on every iteration by 30

        consumer2 = outs(2).zip(P.suspend {
          @volatile var latency1 = 0
          P.repeatEval(Task {
            val init = 500
            latency1 += 30
            val delay = init + latency1
            Thread.sleep(delay)
            delay
          })
        }).map(r ⇒ logger.info(s"${r._2} consumer_2 ${r._1}"))

        _ ← (consumer0 merge consumer1) merge consumer2
      } yield ()

      p.onComplete(P.eval(Task.delay { logger.info("Consumers have done") }))
        .run
        .runAsync(sync.put)

      sync.get
      1 === 1
    }
  }

  def microBatch[I](duration: Duration, maxSize: Int = Int.MaxValue): scalaz.stream.Wye[Long, I, Vector[I]] = {
    import scalaz.stream.ReceiveY.{ HaltOne, ReceiveL, ReceiveR }
    val timeWindow = duration.toNanos

    def go(acc: Vector[I], last: Long): Wye[Long, I, Vector[I]] =
      P.awaitBoth[Long, I].flatMap {
        case ReceiveL(current) ⇒
          if (current - last > timeWindow || acc.size >= maxSize) P.emit(acc) ++ go(Vector(), current)
          else go(acc, last)
        case ReceiveR(i) ⇒
          if (acc.size + 1 >= maxSize) P.emit(acc :+ i) ++ go(Vector(), last)
          else go(acc :+ i, last)
        case HaltOne(e) ⇒
          if (!acc.isEmpty) P.emit(acc) ++ P.Halt(e)
          else P.Halt(e)
      }

    go(Vector(), System.nanoTime)
  }

  val letter = Gen.alphaLowerChar

  def discreteChars(stepMs: Long = 100l): Process[Task, Char] = {
    def go(i: Char): Process[Task, Char] =
      Process.await(Task.delay(i)) { i ⇒
        Thread.sleep(stepMs)
        Process.emit(i) ++ go(letter.sample.getOrElse('a'))
      }
    go(letter.sample.getOrElse('a'))
  }

  def discreteTime(stepMs: Long = 50l): Process[Task, Long] = Process.suspend {
    Process.repeatEval {
      Task.delay { Thread.sleep(stepMs); System.nanoTime }
    }
  }

  "Process stream of symbols" should {
    "have performed symbol count on time window" in {
      import scalaz._
      import Scalaz._
      import scalaz.stream.sink
      val window = FiniteDuration(3, TimeUnit.SECONDS)
      val S = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("micro-batch-wc")))
      val LSink = sink.lift[Task, Map[Char, Int]](map ⇒ Task.delay(logger.info(map)))

      (discreteTime() wye discreteChars())(microBatch(window))(S)
        .map(data ⇒ data.foldMap(i ⇒ Map(i -> 1)))
        .observe(LSink)
        .take(10)
        .foldMonoid
        .to(LSink)
        .onFailure { th ⇒ logger.debug(s"Failure: ${th.getMessage}"); P.halt }
        .onComplete { P.eval(Task.delay(logger.debug(s"Process [symbol-count] has been completed"))) }
        .runLog.run
      1 === 1
    }
  }

  def mergeSorted[T: Ordering](left: List[T], right: List[T])(implicit ord: Ordering[T]): List[T] = {
    val source0 = emitAll(left)
    val source1 = emitAll(right)

    def choose(l: T, r: T): Tee[T, T, T] =
      if (ord.lt(l, r)) emit(l) ++ nextL(r)
      else emit(r) ++ nextR(l)

    def nextR(l: T): Tee[T, T, T] =
      tee.receiveROr[T, T, T](emit(l) ++ tee.passL)(r ⇒ choose(l, r))

    def nextL(r: T): Tee[T, T, T] =
      tee.receiveLOr[T, T, T](emit(r) ++ tee.passR)(l ⇒ choose(l, r))

    def init: Tee[T, T, T] =
      tee.receiveLOr[T, T, T](tee.passR)(nextR)

    (source0 tee source1)(init).toSource.runLog.run.toList
  }

  "MergeSort deterministically 2 sorted list" should {
    "run" in {
      val left = Gen.listOf(Gen.choose(0, 50)).map(_.sorted).sample.getOrElse(Nil)
      val right = Gen.listOf(Gen.choose(0, 50)).map(_.sorted).sample.getOrElse(Nil)
      println(left)
      println(right)

      val result = mergeSorted(left, right)
      println(result)
      result should be equalTo (left ::: right).sorted
    }
  }

  "Back-pressure Fast producer slow consumer" should {
    "dropLast/dropBuffer from the buffer to avoid blocking" in {
      def sleep(latency: Long) = Process.repeatEval(Task.delay(Thread.sleep(latency)))

      val waterMark = 1 << 4
      implicit val Pub = newSingleThreadExecutor(new NamedThreadFactory("producer"))
      implicit val Sub = Strategy.Executor(newFixedThreadPool(3, new NamedThreadFactory("consumer")))

      val buffer = async.boundedQueue[Int](waterMark + 5)(Sub)

      val sinkP = process1.lift[Int, Int] { x ⇒ logger.debug(s"<~ Consumed: $x"); x }
      val dropLast = process1.lift[Int, Int] { x ⇒ logger.debug(s"~> Dropped: $x"); x }
      val dropBuffer = process1.lift[Seq[Int], Int] { seq ⇒ logger.debug(s"~> Dropped buffer: $seq"); 0 }
      val enqueueP = process1.lift[(Int, Unit), Unit] { x ⇒ buffer enqueueOne (x._1) run }

      val sink = (buffer.dequeue zip sleep(300)).map(_._1) pipe sinkP

      val dropLastStrategy = (buffer.size.discrete.filter(_ > waterMark) zip buffer.dequeue).map(_._2) |> dropLast
      val dropBufferStrategy = (buffer.size.discrete.filter(_ > waterMark) zip buffer.dequeueBatch(waterMark)).map(_._2) |> dropBuffer

      Task.fork {
        ((Process.emitAll(1 to 200) zip sleep(200)) |> enqueueP).onComplete(Process.eval_(buffer.close)).run[Task]
      }(Pub).runAsync(_ ⇒ ())

      merge.mergeN(Process(sink, dropLastStrategy))(Sub).runLog.run
      //(src merge merge.mergeN(Process(sink, throttler))).runLog.run
      1 === 1
    }
  }

  "Back-pressure Fast producer slow consumer" should {
    "circularBuffer silently overrides values to avoid blocking" in {
      def sleep(latency: Long) = Process.repeatEval(Task.delay(Thread.sleep(latency)))

      val waterMark = 1 << 4
      implicit val Pub = newSingleThreadExecutor(new NamedThreadFactory("producer"))
      implicit val Sub = Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("consumer")))

      val cBuffer = async.circularBuffer[Int](waterMark)(Sub)
      val consumeP = process1.lift[Int, Int] { x ⇒ logger.debug(s"<~ Consumed: $x"); x }
      val publishP = process1.lift[(Int, Unit), Unit] { x ⇒ (cBuffer enqueueOne (x._1) run) }

      val sink = (cBuffer.dequeue zip sleep(300)).map(_._1) pipe consumeP

      Task.fork {
        ((Process.emitAll(1 to 200) zip sleep(150)) |> publishP).onComplete(Process.eval_(cBuffer.close)).run[Task]
      }(Pub).runAsync(_ ⇒ ())

      sink.runLog.run
      1 === 1
    }
  }


  //this terminates after any side terminate
  def eitherHaltBoth[I, I2]: Wye[I, I2, I \/ I2] =
    wye.receiveBoth {
      case ReceiveL(i)      ⇒ Process.emit(-\/(i)) ++ eitherHaltBoth
      case ReceiveR(i)      ⇒ Process.emit(\/-(i)) ++ eitherHaltBoth
      case HaltL(End)       ⇒ Halt(End)
      case HaltR(End)       ⇒ Halt(End)
      case h @ HaltOne(rsn) ⇒ Halt(rsn)
    }

  val Scheduler = newScheduledThreadPool(1, new NamedThreadFactory("Scheduler"))
  val I = Strategy.Executor(newScheduledThreadPool(2, new NamedThreadFactory("infrastructure")))

  def buildProgress(i: Int) = List.fill(i + 1)(" ★ ").mkString

  def progress(src: Process[Task, String], n: Int)(implicit d: FiniteDuration, S: Strategy): Process[Task, String] = {
    ((src wye time.awakeEvery(d)(I, Scheduler).zipWithIndex.map(_._2 % n))(eitherHaltBoth)(S) |> process1.sliding(2)).flatMap { batch ⇒
      val elements =
        if (batch.size == 1) Seq(batch(0).fold(r ⇒ r, buildProgress(_)))
        else if (batch.forall(_.isRight)) batch.headOption.map(_.fold(r ⇒ r, buildProgress(_))).toSeq
        else batch.filter(_.isLeft).map(_.fold(l ⇒ l, buildProgress(_)))
      Process.emitAll(elements)
    } |> process1.distinctConsecutive[String]
  }

  def naturals(size: Int, stepMs: Long = 3000l): Process[Task, String] = {
    def go(i: Int, sleep: Long, iterN: Int): Process[Task, String] =
      P.await(Task.delay(i)) { i ⇒
        Thread.sleep(stepMs)
        if (iterN > 0) P.emit(i.toString) ++ go(i + 1, ThreadLocalRandom.current().nextLong(1l, stepMs), iterN - 1)
        else Process.halt
      }
    go(1, ThreadLocalRandom.current().nextLong(1l, stepMs), size)
  }

  "track progress" should {
    val S = Strategy.Executor(newFixedThreadPool(2, new NamedThreadFactory("progress")))
    progress(naturals(50), 10)(new FiniteDuration(300, TimeUnit.MILLISECONDS), S)
      .to(sink.lift[Task, String](l ⇒ Task.delay(logger.debug(l))))
      .runLog
      .run

    1 === 1
  }

  "exchange" should {
    "have transferred from InSystem to OutSystem" in {
      val Size = 60
      val I = Strategy.Executor(newFixedThreadPool(4, new NamedThreadFactory("exchange-worker")))
      //values read from remote system
      val InSystem = async.unboundedQueue[Int](I)
      val OutSystem = async.unboundedQueue[Int](I)

      (P.emitAll(1 to Size) to InSystem.enqueue)
        .onComplete(P.eval_(InSystem.close))
        .run
        .runAsync { _ ⇒ logger.info("All input data was written") }

      val Ex = Exchange(InSystem.dequeue, OutSystem.enqueue)

      (Ex.read.map(_ * 2) to Ex.write)
        .onComplete(P.eval { OutSystem.close.map(_ ⇒ Task.delay(logger.info("All input has been transferred"))) })
        .runLog.run

      val buffer = OutSystem.dequeueAvailable.runLog.run(0)
      buffer.size === Size
      buffer.sum === (1 to Size).map(_ * 2).sum
    }
  }
}