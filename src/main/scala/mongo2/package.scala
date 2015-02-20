import java.net.InetSocketAddress

import mongo2.Program.DBInterpreter
import com.mongodb.{ ServerAddress, MongoClient, WriteConcern }
import scala.util.{ Failure, Success, Try }
import scalaz._
import scalaz.concurrent.Task
import scala.language.higherKinds
import com.mongodb.{ DBObject, DBCollection }

package object mongo2 {

  sealed trait RWInstruction[A]
  case class FindOne[A](coll: DBCollection, query: DBObject, next: DBObject ⇒ A) extends RWInstruction[A]
  case class Insert[A](coll: DBCollection, obj: DBObject, next: DBObject ⇒ A) extends RWInstruction[A]

  implicit val functor: Functor[RWInstruction] = new Functor[RWInstruction] {
    def map[A, B](fa: RWInstruction[A])(f: A ⇒ B) =
      fa match {
        case FindOne(c, q, next) ⇒ FindOne(c, q, next andThen f)
        case Insert(c, q, next)  ⇒ Insert(c, q, next andThen f)
      }
  }

  sealed trait LifecycleInstruction[A]
  case class Connect[A](next: Status ⇒ A) extends LifecycleInstruction[A]
  case class Disconnect[A](next: Status ⇒ A) extends LifecycleInstruction[A]

  sealed abstract class Status
  case object Ok extends Status
  case object Error extends Status

  object Program {
    sealed abstract class DBInterpreter[F[_]] {
      def effect[T](action: F[Free[F, T]]): Task[Free[F, T]]
      def transformation: F ~> Task
    }

    implicit val cycle = new DBInterpreter[LifecycleInstruction] {
      override def effect[T](action: LifecycleInstruction[Free[LifecycleInstruction, T]]): Task[Free[LifecycleInstruction, T]] = ???
      override def transformation: ~>[LifecycleInstruction, Task] = ???
    }

    implicit val ReadWrite = new DBInterpreter[RWInstruction] {
      private def transform[T](op: RWInstruction[T]): Task[T] =
        op match {
          case FindOne(c, query, next) ⇒
            Task {
              Option(c.findOne(query)).fold { throw new Exception(s"Can't findOne in $c by $query") } { r ⇒ r }
            } map (next)
          case Insert(c, obj, next) ⇒
            Task {
              Try {
                c.insert(WriteConcern.ACKNOWLEDGED, obj)
                obj
              } match {
                case Success(obj)   ⇒ obj
                case Failure(error) ⇒ throw new Exception(s"Can't Insert in $c - $obj", error)
              }
            } map (next)

        }

      override def effect[T](action: RWInstruction[Free[RWInstruction, T]]): Task[Free[RWInstruction, T]] =
        transform(action)

      override def transformation: RWInstruction ~> Task = new (RWInstruction ~> Task) {
        def apply[T](op: RWInstruction[T]) = transform(op)
      }
    }

    def apply[F[_]](dbName: String, address: InetSocketAddress)(implicit delegate: DBInterpreter[F]) =
      new Program(dbName, address, delegate)
  }

  final class Program[F[_]] private (dbName: String, address: InetSocketAddress, delegate: DBInterpreter[F]) {
    import scalaz.Free.liftF
    private val client = new MongoClient(new ServerAddress(address))

    def findOne(query: DBObject)(implicit collection: String): Free[RWInstruction, DBObject] =
      liftF(FindOne(client.getDB(dbName).getCollection(collection), query, identity))

    def insert(query: DBObject)(implicit collection: String): Free[RWInstruction, DBObject] =
      liftF(Insert(client.getDB(dbName).getCollection(collection), query, identity))

    def effect[T](action: F[Free[F, T]]): Task[Free[F, T]] = delegate.effect(action)

    def transformation: F ~> Task = delegate.transformation
  }
}