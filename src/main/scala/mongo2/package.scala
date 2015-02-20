import java.net.InetSocketAddress

import com.mongodb.{ ServerAddress, MongoClient, WriteConcern }
import mongo2.Program.DBInterpreter
import scala.util.{ Failure, Success, Try }
import scalaz.{ Free, Functor }
import scalaz.concurrent.Task
import scala.language.higherKinds
import com.mongodb.{ DBObject, DBCollection }

package object mongo2 {

  implicit val functor: Functor[InteractionAlgebra] = new Functor[InteractionAlgebra] {
    def map[A, B](fa: InteractionAlgebra[A])(f: A ⇒ B) =
      fa match {
        case FindOne(c, q, next) ⇒ FindOne(c, q, next andThen f)
        case Insert(c, q, next)  ⇒ Insert(c, q, next andThen f)
      }
  }

  sealed abstract class InteractionAlgebra[A]

  final case class FindOne[A](coll: DBCollection, query: DBObject, next: DBObject ⇒ A) extends InteractionAlgebra[A]

  implicit val functorF = new Functor[FindOne] {
    def map[A, B](fa: FindOne[A])(f: A ⇒ B) = FindOne(fa.coll, fa.query, fa.next andThen f)
  }

  implicit val functorI = new Functor[Insert] {
    def map[A, B](fa: Insert[A])(f: A ⇒ B) = Insert(fa.coll, fa.obj, fa.next andThen f)
  }

  final case class Insert[A](coll: DBCollection, obj: DBObject, next: DBObject ⇒ A) extends InteractionAlgebra[A] {
    def map[B](f: (A) ⇒ B): InteractionAlgebra[B] = copy(next = next andThen f)
  }

  object Program {
    sealed abstract class DBInterpreter[F[_]: Functor] {
      def effect[T](action: F[Free[F, T]]): Task[Free[F, T]]
    }

    implicit val ReadWrite = new DBInterpreter[InteractionAlgebra] {
      override def effect[T](action: InteractionAlgebra[Free[InteractionAlgebra, T]]): Task[Free[InteractionAlgebra, T]] =
        action match {
          case FindOne(c, o, next) ⇒ {
            Task {
              Try {
                c.insert(WriteConcern.ACKNOWLEDGED, o)
                o
              } match {
                case Success(obj)   ⇒ obj
                case Failure(error) ⇒ throw new Exception(s"Can't Insert in $c - $o", error)
              }
            } map (next)
          }
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
    }

    def apply[F[_]](dbName: String, address: InetSocketAddress)(implicit delegate: DBInterpreter[F]) =
      new Program(dbName, address, delegate)
  }

  final class Program[F[_]] private (dbName: String, address: InetSocketAddress, delegate: DBInterpreter[F]) {
    import scalaz.Free.liftF
    private val client = new MongoClient(new ServerAddress(address))

    def findOne(query: DBObject)(implicit collection: String): Free[InteractionAlgebra, DBObject] =
      liftF(FindOne(client.getDB(dbName).getCollection(collection), query, identity))

    def insert(query: DBObject)(implicit collection: String): Free[InteractionAlgebra, DBObject] =
      liftF(Insert(client.getDB(dbName).getCollection(collection), query, identity))

    def effect[T](action: F[Free[F, T]]): Task[Free[F, T]] = delegate.effect(action)
  }
}
