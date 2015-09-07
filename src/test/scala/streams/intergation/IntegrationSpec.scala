package streams.intergation

import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnNext }
import akka.stream.scaladsl._
import akka.actor.{ Props, ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.concurrent.AsyncAssertions.Waiter
import akka.stream.actor.{ OneByOneRequestStrategy, ActorSubscriber, ActorPublisher }
import akka.stream.{ Attributes, ActorMaterializer, ActorMaterializerSettings }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpecLike }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.SyncVar
import scala.util.{ Try, Success, Failure }
import scalaz.concurrent.Task
import scalaz.stream._

class IntegrationSpec extends TestKit(ActorSystem("integration"))
    with WordSpecLike with MustMatchers
    with BeforeAndAfterEach with BeforeAndAfterAll
    with ImplicitSender {

  val P = Process

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  object AkkaContext {
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system)
      .withDispatcher("akka.flow-dispatcher"))
    implicit val E = streams.ExecutionContextExecutorServiceBridge(materializer.executionContext)
  }

  import AkkaContext._
  import streams._

  val limit = 153
  val range = 0 to limit

  "Scalaz-Streams toAkkaFlow" must {
    "run" in {
      import scalaz._
      import Scalaz._

      //count ack messages
      implicit val M = Monoid[Int]
      val ssSource = P.emitAll(range).toSource

      ssSource.toAkkaFlow
        .fold1Map(_ ⇒ 1)
        .runLog.run must be === Vector(range.size)
    }
  }

  "Scalaz-Stream process through ActorPublisher ActorSubscriber one by one" must {
    "run" in {
      P.emitAll(range).sourceToSink.runLog.run must be === range.toVector
    }
  }

  /**
   * Zip 2 scalaz-stream processes with akka Zip
   * +-------------+
   * |Process(odd) |---+
   * +-------------+   | +------------------+
   *                   +-|Akka Source (Zip) |
   * +-------------+   | +------------------+
   * |Process(even)|---+
   * +-------------+
   */
  "Scalaz-Stream processes to Akka Source" must {
    "run" in {
      val sync = new SyncVar[Boolean]

      //act like tee, deterministically merge
      def zip[T](left: ActorRef, right: ActorRef): Source[(T, T), Unit] =
        Source() { implicit builder ⇒
          import FlowGraph.Implicits._
          val zip = builder.add(Zip[T, T]())
          Source(ActorPublisher[T](left)) ~> zip.in0
          Source(ActorPublisher[T](right)) ~> zip.in1
          zip.out
        }

      val odd = P.emitAll(range).toSource.filter(_ % 2 != 0)
      val even = P.emitAll(range).toSource.filter(_ % 2 == 0)

      val Podd = system.actorOf(streams.BatchWriter.props[Int])
      val Peven = system.actorOf(streams.BatchWriter.props[Int])

      (odd to Podd.writer[Int]).run.runAsync(_ ⇒ ())
      (even to Peven.writer[Int]).run.runAsync(_ ⇒ ())

      val zippedSource = zip[Int](Podd, Peven).map(pair ⇒ s"${pair._1} - ${pair._2}")

      zippedSource
        .toMat(Sink.foreach(x ⇒ println(s"pair: $x")))(Keep.right)
        .run()
        .onComplete { _ ⇒ sync.put(true) }

      sync.get must be === true
    }
  }

  /**
   * Broadcast from scalaz-stream process to akka flow
   *                        +------------+
   *                    +---|Sink.foreach|
   * +--------------+   |   +------------+
   * |Process(range)|---+
   * +--------------+   |   +------------+
   *                    +---|Sink.foreach|
   *                        +------------+
   */
  "Scalaz-Stream process throw Akka Flow" must {
    "run fan-in operation" in {
      val w = new Waiter

      val digits: Process[Task, Int] = P.emitAll(range)
      val p = system.actorOf(streams.BatchWriter.props[Int])
      (digits to p.writer[Int]).run.runAsync(_ ⇒ ())

      val leftSink = Sink.foreach[Int](x ⇒ println(s"left: $x"))
      val rightSink = Sink.foreach[Int](x ⇒ println(s"right: $x"))

      val (lf, rf) = FlowGraph.closed(leftSink, rightSink)((_, _)) { implicit b ⇒
        (lout, rout) ⇒
          import FlowGraph.Implicits._
          val bcast = b.add(Broadcast[Int](2))
          Source(ActorPublisher[Int](p)) ~> bcast
          bcast ~> lout
          bcast ~> rout
      }.run()

      lf zip rf onComplete {
        case Success(r)  ⇒ w.dismiss()
        case Failure(ex) ⇒ throw ex
      }
      w.await()
    }
  }

  /**
   * Broadcast from akka source to scalaz-stream
   *                              +------------------+
   *                         +----| Sink.foreach     |
   *                         |    +------------------+
   * +-------------------+   |    +------------------+
   * |(Akka)Source(range)|---+----| Process(subEven) |
   * +-------------------+   |    +------------------+
   *                         |    +------------------+
   *                         +----| Process(subPrime)|
   *                              +------------------+
   */
  "Akka Flow to Scalaz-Stream process" must {
    "run" in {
      val evenSync = new SyncVar[Boolean]()
      val primeSync = new SyncVar[Boolean]()

      def isPrime(n: Int) = {
        def primes = sieve(from(2))
        def from(n: Int): Stream[Int] = n #:: from(n + 1)
        def sieve(s: Stream[Int]): Stream[Int] =
          s.head #:: sieve(s.tail filter (_ % s.head != 0))

        lazy val primesN = primes.take(limit)
        primesN.contains(n)
      }

      val evenR = system.actorOf(Reader.props[Int], "even-reader")
      val primeR = system.actorOf(Reader.props[Int], "prime-reader")

      val odd = akka.stream.scaladsl.Sink.foreach[Int](v ⇒ println(s"odd $v"))
      val even = akka.stream.scaladsl.Sink(ActorSubscriber[Int](evenR))
      val prime = akka.stream.scaladsl.Sink(ActorSubscriber[Int](primeR))

      val src = akka.stream.scaladsl.Source(range)

      FlowGraph.closed() { implicit b ⇒
        import FlowGraph.Implicits._
        val bcast = b.add(Broadcast[Int](3))
        src ~> bcast.in
        bcast.out(0) ~> Flow[Int].filter { _ % 2 != 0 } ~> odd
        bcast.out(1) ~> Flow[Int].filter { _ % 2 == 0 } ~> even
        bcast.out(2) ~> Flow[Int].filter { isPrime } ~> prime
      }.run

      evenR.reader[Int].map { x ⇒
        println(s"even $x")
        x
      }.take(limit / 2).run.runAsync(_ ⇒ evenSync.put(true))

      primeR.reader[Int].map { x ⇒
        println(s"prime $x")
        x
      }.take(25).run.runAsync(_ ⇒ primeSync.put(true))

      primeSync.get must be === true
      evenSync.get must be === true
    }
  }

  /**
   *
   * +---------------+    +-----+   +------------------------+
   * |Process[Task,T]|----|Flow |---|Process[Task, Vector[T]]|
   * +---------------+    +-----+   +------------------------+
   *
   */
  "Scalaz-Stream process through Akka Flow with batching" must {
    "run" in {
      val size = 8 //
      val source: Process[Task, Int] = P.emitAll(range)

      //The example below will ensure that size*2 jobs (but not more) are enqueue from an external process and stored locally in memory
      val flow = Flow[Int].map(_ * 2)
        .withAttributes(Attributes.inputBuffer(initial = size * 2, max = size * 2))

      source.throughAkkaFlow(size, flow).fold1(_ ++ _)
        .runLog.run must be === Vector(range.toVector.map(_ * 2))
    }
  }

  /*
   *                                    +-----+
   *                                 +--|Sink1|
   *                                 |  +-----+
   * +----------+      +----------+  |  +-----+    +--------+
   * |Process   |------|AkkaSource|-----|Flow |----|AkkaSink|
   * +----------+      +----------+  |  +-----+    +--------+
   *                                 |  +-----+
   *                                 +__|Sink0|
   *                                    +-----+
   *
   */
  "Scalaz-Stream process to AkkaSource through Flow to AkkaSink" must {
    "run" in {
      val size = 8
      val sync = new SyncVar[Try[Int]]()

      val akkaSource = P.emitAll(range).toAkkaSource
      val foldSink = Sink.fold[Int, Int](0)(_ + _)

      val actorSink0 = Sink.actorSubscriber(Props(new SlowingDownActor("DegradingActor0", 5l, 0l)))
      val actorSink1 = Sink.actorSubscriber(Props(new SlowingDownActor("DegradingActor1", 7l, 0l)))

      val flow = Flow[Int].map { x ⇒ println(Thread.currentThread().getName); x * 2 }
        .withAttributes(Attributes.inputBuffer(initial = size * 2, max = size * 2))

      FlowGraph.closed(akkaSource, foldSink)((_, _)) { implicit b ⇒
        import FlowGraph.Implicits._
        (src, sink) ⇒
          val broadcast = b.add(Broadcast[Int](3))
          src ~> broadcast ~> actorSink0
          broadcast ~> actorSink1
          broadcast ~> flow ~> sink
      }.run()._2.onComplete(r ⇒ sync.put(r))

      sync.get must be === Success(range.sum * 2)
    }
  }

  class SlowingDownActor(name: String, delayPerMsg: scala.Long, initialDelay: scala.Long) extends ActorSubscriber {
    private var delay = 0l
    override protected val requestStrategy = OneByOneRequestStrategy

    def this(name: String) {
      this(name, 0l, 0l)
    }

    def this(name: String, delayPerMsg: Long) {
      this(name, delayPerMsg, 0l)
    }

    override def receive: Receive = {
      case OnNext(msg: Int) ⇒
        println(s"${Thread.currentThread.getName}: $name $msg")
        delay += delayPerMsg
        Thread.sleep(initialDelay + (delay / 1000), delay % 1000 toInt)

      case OnComplete ⇒
        println("Complete DegradingActor")
        context.system.stop(self)
    }
  }
}