import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import akka.actor.{ ActorRefFactory, PoisonPill, ActorRef }
import akka.stream.ActorFlowMaterializer
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnError }
import akka.stream.actor.{ ActorSubscriber, ActorPublisher }
import mongo.MongoProgram.NamedThreadFactory
import streams.BatchWriter.WriterDone
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }
import scalaz.{ \/, -\/, \/- }
import scalaz.concurrent.Task
import scalaz.stream._
import akka.pattern.ask

package object streams { outer ⇒

  val P = Process

  case class ReadData[I](cb: \/[Throwable, I] ⇒ Unit)
  case class ReadBatchData[I](cb: \/[Throwable, Vector[I]] ⇒ Unit)
  case class WriteRequest[I](cb: \/[Throwable, Unit] ⇒ Unit, i: I)

  implicit class ProcessSyntax[I](val self: Process[Task, I]) extends AnyVal {

    def toAkkaFlow(implicit actorRefFactory: ActorRefFactory, materializer: ActorFlowMaterializer): Process[Task, Unit] =
      outer.createSink(self)

    def toAkkaFlow(a: ActorRef)(implicit actorRefFactory: ActorRefFactory, materializer: ActorFlowMaterializer): Process[Task, Unit] =
      outer.createSink0(a, self)

    def throughAkkaFlow(implicit actorRefFactory: ActorRefFactory, materializer: ActorFlowMaterializer): Process[Task, I] =
      outer.createChain(self)

    def throughBufferedAkkaFlow(batchSize: Int)(implicit actorRefFactory: ActorRefFactory, materializer: ActorFlowMaterializer): Process[Task, Vector[I]] =
      outer.createBufferedChain(self, batchSize)
  }

  private def createBufferedChain[I](process: Process[Task, I], batchSize: Int)(implicit arf: ActorRefFactory, m: ActorFlowMaterializer): Process[Task, Vector[I]] = {
    def halter(pub: ActorRef, sub: ActorRef): Cause ⇒ Process[Task, Vector[I]] = {
      case cause @ Cause.End ⇒
        P.eval(Task.async { cb: (\/[Throwable, Vector[I]] ⇒ Unit) ⇒ sub ! ReadBatchData[I](cb) }).onHalt({
          case cause @ Cause.End ⇒
            sub ! OnComplete
            Process.Halt(cause)
          case cause @ Cause.Kill ⇒
            pub ! OnError(new Exception("Process killed"))
            Process.Halt(cause)
          case cause @ Cause.Error(ex) ⇒
            pub ! OnError(ex)
            Process.Halt(cause)
        })
      case cause @ Cause.Kill ⇒
        pub ! OnError(new Exception("Process killed"))
        Process.Halt(cause)
      case cause @ Cause.Error(ex) ⇒
        pub ! OnError(ex)
        Process.Halt(cause)
    }

    val pub = arf.actorOf(SourceBatchedPublisher.props[I], name = "buffer-seq-pub")
    val sub = arf.actorOf(SinkBatchedSubscriber.props[I](batchSize), name = "buffer-seq-sub")
    akka.stream.scaladsl.Source(ActorPublisher[I](pub)).to(akka.stream.scaladsl.Sink(ActorSubscriber[I](sub))).run()

    val i = new AtomicLong()
    (for {
      p ← (process to sinkReader[I](pub)).splitWith { in ⇒ { i.incrementAndGet() % batchSize == 0 } }.filter(_.size == 1)
      i ← P.eval(Task.async { cb: (\/[Throwable, Vector[I]] ⇒ Unit) ⇒ sub ! ReadBatchData[I](cb) })
    } yield i).onHalt(halter(pub, sub))
  }

  private def createChain[I](process: Process[Task, I])(implicit arf: ActorRefFactory, m: ActorFlowMaterializer): Process[Task, I] = {
    val pub = arf.actorOf(SequentialSourcePublisher.props[I], name = "seq-pub")
    val sub = arf.actorOf(SequentualSinkSubscriber.props[I], name = "seq-sub")
    val src = akka.stream.scaladsl.Source(ActorPublisher[I](pub))
    val sink = akka.stream.scaladsl.Sink(ActorSubscriber[I](sub))
    src.to(sink).run()

    (for {
      _ ← (process to sinkReader[I](pub))
      r ← P.eval(Task.async { cb: (\/[Throwable, I] ⇒ Unit) ⇒ sub ! ReadData[I](cb) })
    } yield r).onHalt({
      case cause @ Cause.End ⇒
        sub ! OnComplete
        Process.Halt(cause)
      case cause @ Cause.Kill ⇒
        pub ! OnError(new Exception("Process killed"))
        Process.Halt(cause)
      case cause @ Cause.Error(ex) ⇒
        pub ! OnError(ex)
        Process.Halt(cause)
    })
  }

  private def createSink0[I](a: ActorRef, process: Process[Task, I])(implicit actorRefFactory: ActorRefFactory, materializer: ActorFlowMaterializer): Process[Task, Unit] = {
    process.onFailure { ex ⇒
      a ! Cancel
      P.halt
    }.to(sink[I](a))
  }

  private def createSink[I](process: Process[Task, I])(implicit actorRefFactory: ActorRefFactory, materializer: ActorFlowMaterializer): Process[Task, Unit] = {
    val processor = actorRefFactory.actorOf(ProcessorSink.props[I], name = "sink-proc")
    process.onFailure { ex ⇒
      processor ! Cancel
      P.halt
    }.to(sink[I](processor))
  }

  private def sink[I](processor: ActorRef)(implicit materializer: ActorFlowMaterializer): Sink[Task, I] = {
    akka.stream.scaladsl.Source(ActorPublisher[I](processor))
      .to(akka.stream.scaladsl.Sink(ActorSubscriber[I](processor))).run()
    sinkReader(processor)
  }

  private def sinkReader[I](pub: ⇒ ActorRef): Sink[Task, I] = {
    io.resource[Task, ActorRef, I ⇒ Task[Unit]] { Task.delay[ActorRef](pub) } { pub ⇒ Task.delay(()) } { pub ⇒
      Task.delay(i ⇒ Task.async[Unit](cb ⇒ pub ! WriteRequest(cb, i)))
    }
  }

  private def writer[I](pub: ActorRef): scalaz.stream.Sink[Task, I] = {
    io.resource[Task, ActorRef, I ⇒ Task[Unit]] { Task.delay[ActorRef](pub) } { pub ⇒ Task.delay(pub ! WriterDone) } { pub ⇒ Task.delay(i ⇒ Task.async[Unit](cb ⇒ pub ! WriteRequest(cb, i))) }
  }

  /*case class ChannelAcknowledge[I](cb: \/[Throwable, I] ⇒ Unit, i: I)
  private def toChannel[I](pub: ActorRef): Channel[Task, I, I] = {
    io.resource[Task, ActorRef, I ⇒ Task[I]] { Task.delay[ActorRef](pub) } { adapterActor ⇒ Task.delay(()) } { pub ⇒
      Task.delay(i ⇒ Task.async[I](cb ⇒ pub ! ChannelAcknowledge(cb, i)))
    }
  }*/

  import Executors._
  import scala.concurrent._
  implicit val ec = ExecutionContext.fromExecutor(newFixedThreadPool(2, new NamedThreadFactory("future-worker")))

  implicit class FutureOps[+A](f: ⇒ Future[A]) {
    def toTask: Task[A] = Task async { cb ⇒
      f.onComplete {
        case Success(v) ⇒ cb(\/-(v))
        case Failure(e) ⇒ cb(-\/(e))
      }
    }
  }

  implicit class ActorRefSyntax(val self: ActorRef) extends AnyVal {

    def reader[I]: Process[Task, I] =
      outer.reader(self)

    def writer[I]: Sink[Task, I] =
      outer.writer(self)

    def akkaChannel[A, B](timeout: FiniteDuration = 10.seconds)(implicit tag: ClassTag[B]): scalaz.stream.Channel[Task, A, B] = {
      implicit val t = akka.util.Timeout(timeout)
      outer.requestor[A, B](self)
    }
    def requestRes[A, B](timeout: FiniteDuration = 10.seconds)(implicit tag: ClassTag[B]) = {
      implicit val t = akka.util.Timeout(timeout)
      io.resource(Task.delay(self))(a ⇒ Task.delay(a ! PoisonPill)) { a ⇒
        Task delay { m: A ⇒ (a ask m).mapTo[B] toTask }
      }
    }
  }

  def reader[I](sub: ActorRef): scalaz.stream.Process[Task, I] = {
    io.resource[Task, ActorRef, I] { Task.delay[ActorRef](sub) } { sub ⇒ Task.delay(()) } { sub ⇒ Task.async(cb ⇒ sub ! ReadData[I](cb)) }
  }

  def requestor[A, B](actor: ActorRef)(implicit timeout: akka.util.Timeout, tag: ClassTag[B]): scalaz.stream.Channel[Task, A, B] = {
    scalaz.stream.channel.lift[Task, A, B] { message: A ⇒
      Task suspend { (actor ask message).mapTo[B] toTask }
    }
  }
}