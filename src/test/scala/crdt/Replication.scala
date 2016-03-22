package crdt

import java.util.concurrent.Executors._
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import com.rbmhtechnology.eventuate.{ ConcurrentVersionsTree, Versioned, VectorTime, crdt }

import scalaz.stream.async
import scala.language.higherKinds
import org.scalacheck.{ Arbitrary, Gen }
import scala.collection.concurrent.TrieMap
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.async.mutable.{ Signal, Queue }

object Replication {

  val ShoppingCartLog = org.apache.log4j.Logger.getLogger("ShoppingCart")

  val Size = 10
  val P = scalaz.stream.Process
  val replicasN = Set(1, 2, 3, 4)

  //optimal size is equal to the number of replica
  val R = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors() / 2,
    new RThreadFactory("worker")))

  final class RThreadFactory(var name: String) extends ThreadFactory {
    private def namePrefix = name + "-thread"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    override def newThread(r: Runnable) = new Thread(this.group, r,
      s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
  }

  def genBoundedList[T](size: Int, g: Gen[T]): Gen[List[T]] = Gen.listOfN(size, g)

  def genBoundedVector[T](size: Int, g: Gen[T]) = Gen.containerOfN[Vector, T](size, g)

  implicit def DropArbitrary: org.scalacheck.Arbitrary[List[Int]] =
    Arbitrary(genBoundedList[Int](Size / 2, Gen.chooseNum(0, Size - 1)))

  //for Debug
  implicit def BuyArbitraryDebug: org.scalacheck.Arbitrary[Vector[String]] =
    Arbitrary(genBoundedVector[String](Size,
      for {
        a ← Gen.alphaUpperChar
        b ← Gen.alphaLowerChar
        c ← Gen.alphaUpperChar
      } yield new String(Array(a, b, c))
    ))

  //TODO:
  trait AsyncReplica[F[_], T] {
    def update(): Unit
    def replicate(): Unit
  }

  trait Replica[F[_], T] {
    protected val ADD = """add-(.+)""".r
    protected val DROP = """drop-(.+)""".r

    protected var replicaNum: Int = 0
    protected var queue: Queue[T] = null
    protected var replicationSignal: Signal[F[T]] = null

    private def init(replicaNum0: Int, queue0: Queue[T], signal: Signal[F[T]]): Replica[F, T] = {
      replicaNum = replicaNum0
      queue = queue0
      replicationSignal = signal
      this
    }

    def converge(op: T, s: F[T]): F[T]

    protected def lookup(crdt: F[T]): Set[T]

    /**
     * Total order
     */
    def run(sink: TrieMap[Int, Set[T]]): Task[Unit] = {
      queue.dequeue.map { operation ⇒
        replicationSignal.compareAndSet { state ⇒
          Option(converge(operation, state.get))
        }.runAsync(_ ⇒ ())
      }.onComplete(P.eval(Task.now(sink += replicaNum -> lookup(replicationSignal.get.run))))
        .run[Task]
    }
  }

  case class LWWCrdtState[T](crdt: io.dmitryivanov.crdt.sets.LWWSet[T], cnt: Long = 0l)

  trait OrderEvent { def orderId: String }
  case class OrderCreated(orderId: String, creator: String = "") extends OrderEvent
  case class OrderItemAdded(orderId: String, item: String) extends OrderEvent
  case class OrderItemRemoved(orderId: String, item: String) extends OrderEvent

  case class Order(id: String, items: List[String] = Nil) {
    def addLine(item: String): Order = copy(items = item :: items)
    def removeLine(item: String): Order = copy(items = items.filterNot(_ == item))
    override def toString() = s"[${id}] items=${items.reverse.mkString(",")}"
  }

  val updater: (Order, OrderEvent) ⇒ Order = {
    case (_, OrderCreated(orderId, _))            ⇒ Order(orderId)
    case (order, OrderItemAdded(orderId, item))   ⇒ order.addLine(item)
    case (order, OrderItemRemoved(orderId, item)) ⇒ order.removeLine(item)
  }

  case class ConcurrentVersionsState[T](cv: ConcurrentVersionsTree[Order, T] = ConcurrentVersionsTree(updater).update(
                                          OrderCreated("shopping-cart-qwerty"),
                                          VectorTime("replica-1" -> 1l, "replica-2" -> 1l, "replica-3" -> 1l, "replica-4" -> 1l)
                                        ).asInstanceOf[ConcurrentVersionsTree[Order, T]])

  object Replica {

    def apply[F[_], T](num: Int, opsQ: Queue[T], replicationSignal: Signal[F[T]])(implicit replica: Replica[F, T]): Replica[F, T] =
      replica.init(num, opsQ, replicationSignal)

    implicit def eventuateCrdt = akka.contrib.datareplication.Replicas.eventuateReplica()
    implicit def akkaCrdt = akka.contrib.datareplication.Replicas.akkaReplica()

    //ORSet doesn't fix because to be able to remove product you have to perform remove operation on
    //the same node where add was executed
    //implicit def scalaORSet = akka.contrib.datareplication.Replicas.scalaCrdtORSet()

    implicit def scalaLWWSet = akka.contrib.datareplication.Replicas.scalaCrdtLWWSet()
    implicit def scalaLWWState = akka.contrib.datareplication.Replicas.scalaCrdtLWWState()
    implicit def eventuateCv = akka.contrib.datareplication.Replicas.eventuateConcurrentVersions()
  }

  implicit val zeroEventuateORSet = com.rbmhtechnology.eventuate.crdt.ORSet[String]()
  implicit val zeroAkkaORSet = akka.contrib.datareplication.ORSet.empty[String]
  implicit val zeroScalaLWWSet = new io.dmitryivanov.crdt.sets.LWWSet[String]()
  implicit val zeroScalaLLWState = LWWCrdtState(new io.dmitryivanov.crdt.sets.LWWSet[String](), 0l)
  implicit val zeroCVState = ConcurrentVersionsState[OrderEvent]()

  def replicationSignal[F[_], T](S: Strategy)(implicit zero: F[T]) = async.signalOf[F[T]](zero)(S)
}