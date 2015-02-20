package mongo

import java.net.InetSocketAddress
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import de.bwaldvogel.mongo.MongoServer
import scala.util.{ Failure, Success, Try }
import scalaz.Free.liftF
import com.mongodb._
import scalaz.concurrent.Task
import scalaz._

trait InterpreterOps[T] {
  def apply(): T
}

object MongoProgram {

  type DBFree[T] = Free[MongoAlgebra, T]

  sealed trait MongoAlgebra[+A] {
    def map[B](f: A ⇒ B): MongoAlgebra[B]
  }

  implicit val functor: Functor[MongoAlgebra] = new Functor[MongoAlgebra] {
    def map[A, B](fa: MongoAlgebra[A])(f: A ⇒ B): MongoAlgebra[B] = fa map f
  }

  case class FindOne[+A](coll: DBCollection, query: DBObject, next: DBObject ⇒ A) extends MongoAlgebra[A] {
    def map[B](f: A ⇒ B): MongoAlgebra[B] =
      copy(next = x ⇒ f(next(x)))
  }

  case class Insert[+A](coll: DBCollection, obj: DBObject, next: DBObject ⇒ A) extends MongoAlgebra[A] {
    def map[B](f: A ⇒ B): MongoAlgebra[B] =
      copy(next = next andThen f)
  }

  case class Fail[A](cause: Throwable) extends MongoAlgebra[A] {
    def map[B](fn: A ⇒ B): MongoAlgebra[B] = Fail[B](cause)
  }

  /**
   *
   * @param dbName
   * @param address
   * @param ops
   * @tparam T
   * @return
   */
  def program[T <: Interpreter](implicit dbName: String, address: InetSocketAddress, ops: InterpreterOps[T]) =
    new MongoProgram(dbName, address, ops())

  /**
   * Available for test only
   * @param server
   * @param f
   * @param ops
   * @tparam T
   * @return
   */
  private[mongo] def withTestMongoServer[T <: Interpreter](server: MongoServer)(f: MongoProgram[T] ⇒ Unit)(implicit ops: InterpreterOps[T]) = {
    val dbName = "test_db"
    val addr = server.bind
    val core = program[T](dbName, addr, ops)
    f(core)
    server.shutdownNow
  }

  final class NamedThreadFactory(var name: String) extends ThreadFactory {
    private def namePrefix = name + "-thread"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup()

    override def newThread(r: Runnable) = new Thread(this.group, r,
      s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
  }
}

/**
 * @param dbName
 * @param address
 *
 */
final class MongoProgram[T <: Interpreter] private (dbName: String, address: InetSocketAddress, interpreter: T) {
  import MongoProgram._

  private val delegate = interpreter
  private val client = new MongoClient(new ServerAddress(address))

  /**
   * *
   * @param obj
   * @param collection
   * @return
   */
  def insert[T](obj: DBObject)(implicit collection: String): DBFree[DBObject] =
    liftF(Insert(client.getDB(dbName).getCollection(collection), obj, identity))

  /**
   * *
   * @param query
   * @param collection
   * @return
   */
  def findOne[T](query: DBObject)(implicit collection: String): DBFree[DBObject] =
    liftF(FindOne(client.getDB(dbName).getCollection(collection), query, identity))

  /**
   *
   * @param action
   * @tparam T
   * @return
   */
  def effect[T](action: MongoAlgebra[DBFree[T]]): Task[DBFree[T]] =
    delegate.effect(action)

  /**
   *
   * @param program
   * @param actions
   * @tparam T
   * @return
   */
  def instructions[T](program: DBFree[T], actions: List[String] = Nil): List[String] =
    delegate.instructions(program, actions)
  /**
   *
   *
   */
  implicit val mongoFreeMonad: Monad[DBFree] = new Monad[DBFree] {
    def point[A](a: ⇒ A) = Free.point(a)
    def bind[A, B](action: DBFree[A])(f: A ⇒ DBFree[B]) = action flatMap f
  }
}