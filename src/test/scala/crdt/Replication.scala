package crdt

import java.util.concurrent.Executors._

import scalaz.stream.async
import scala.language.higherKinds
import org.scalacheck.{ Arbitrary, Gen }
import scala.collection.concurrent.TrieMap
import mongo.MongoProgram.NamedThreadFactory
import scalaz.concurrent.{ Task, Strategy }
import scalaz.stream.async.mutable.{ Signal, Queue }

object Replication {

  val Size = 20
  val P = scalaz.stream.Process
  val replicasN = Set(1, 2, 3, 4)

  //optimal size is equal to the number of replica
  val R = Strategy.Executor(newFixedThreadPool(Runtime.getRuntime.availableProcessors() / 2,
    new NamedThreadFactory("worker")))

  /*"VectorTime" should {
    "have detected concurrent versions" in {
      val r0 = VectorTime("a" -> 1l, "b" -> 0l) conc VectorTime("a" -> 0l, "b" -> 1l)
      val r1 = VectorTime("a" -> 1l) conc VectorTime("b" -> 1l)
      r0 === r1 //true
    }
  }*/

  def genBoundedList[T](size: Int, g: Gen[T]): Gen[List[T]] = Gen.listOfN(size, g)

  def genBoundedVector[T](size: Int, g: Gen[T]) = Gen.containerOfN[Vector, T](size, g)

  implicit def DropArbitrary: org.scalacheck.Arbitrary[List[Int]] =
    Arbitrary(genBoundedList[Int](Size / 2, Gen.chooseNum(0, Size - 1)))

  implicit def BuyArbitrary: org.scalacheck.Arbitrary[Vector[String]] =
    Arbitrary(genBoundedVector[String](Size, Gen.uuid.map { _.toString }))

  trait Replica[F[_], T] {
    protected val ADD = """add-(.+)""".r
    protected val DROP = """drop-(.+)""".r

    protected var num: Int = 0
    private var input: Queue[T] = null
    private var replicator: Signal[F[T]] = null

    private def create(n: Int, i: Queue[T], r: Signal[F[T]]): Replica[F, T] = {
      num = n
      input = i
      replicator = r
      this
    }

    def merge(cmd: T, s: F[T]): F[T]

    protected def elements(set: F[T]): Set[T]

    def task(collector: TrieMap[Int, Set[T]]): Task[Unit] =
      input.dequeue.flatMap { action ⇒
        P.eval(replicator.compareAndSet(c ⇒ Some(merge(action, c.get))))
        /*zip P.eval(replicator.get))
          .map(out ⇒ LoggerI.info(s"Replica:$numR Order:${out._2.value} VT:[${out._2.versionedEntries}] Local-VT:[$localTime]"))*/
      }.onComplete(P.eval(Task.now(collector += num -> elements(replicator.get.run))))
        .run[Task]
  }

  object Replica {

    def apply[F[_], T](n: Int, i: Queue[T], S: Strategy)(implicit replica: Replica[F, T], zero: F[T]): Replica[F, T] =
      replica.create(n, i, async.signalOf[F[T]](zero)(S))

    def apply[F[_], T](n: Int, i: Queue[T], replicator: Signal[F[T]])(implicit replica: Replica[F, T]): Replica[F, T] =
      replica.create(n, i, replicator)

    implicit def eventuateR = akka.contrib.datareplication.Replicas.eventuateReplica()
    implicit def akkaR = akka.contrib.datareplication.Replicas.akkaReplica()
  }

  implicit val evenSet = com.rbmhtechnology.eventuate.crdt.ORSet[String]()
  implicit val akkaSet = akka.contrib.datareplication.ORSet.empty[String]

  def replicatorFor[F[_], T](S: Strategy)(implicit zero: F[T]) = async.signalOf[F[T]](zero)(S)
}
