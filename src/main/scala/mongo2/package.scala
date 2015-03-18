import java.net.InetSocketAddress

import mongo2.Program.DBInterpreter
import com.mongodb._
import org.apache.log4j.Logger
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }
import scalaz._
import scalaz.concurrent.Task
import scalaz.Free.liftF
import scala.language.higherKinds
import scalaz.effect.IO

package object mongo2 {

  sealed trait MongoIO[+A]
  case class FindOne[A](coll: DBCollection, query: DBObject, next: DBObject ⇒ A) extends MongoIO[A]
  case class Insert[A](coll: DBCollection, obj: DBObject, next: DBObject ⇒ A) extends MongoIO[A]

  implicit val functor: Functor[MongoIO] = new Functor[MongoIO] {
    def map[A, B](fa: MongoIO[A])(f: A ⇒ B) =
      fa match {
        case FindOne(c, q, next) ⇒ FindOne(c, q, next andThen f)
        case Insert(c, q, next)  ⇒ Insert(c, q, next andThen f)
      }
  }

  implicit val functorF: Functor[FindOne] = new Functor[FindOne] {
    override def map[A, B](fa: FindOne[A])(f: (A) ⇒ B): FindOne[B] =
      FindOne(fa.coll, fa.query, x ⇒ f(fa.next(x)))
  }

  implicit val functorI: Functor[Insert] = new Functor[Insert] {
    override def map[A, B](fa: Insert[A])(f: (A) ⇒ B): Insert[B] =
      Insert(fa.coll, fa.obj, x ⇒ f(fa.next(x)))
  }

  sealed trait Lifecycle[A]
  case class Connect[A](next: Status ⇒ A) extends Lifecycle[A]
  case class Disconnect[A](next: Status ⇒ A) extends Lifecycle[A]

  sealed abstract class Status
  case object Ok extends Status
  case object Error extends Status

  object Program {

    sealed abstract class DBInterpreter[F[_]] {
      def effect[T](action: F[Free[F, T]]): Task[Free[F, T]]
      def toTask: F ~> Task
      def toIO: F ~> IO
      def toInstructions[T](exp: Free[F, T]): List[String]
    }

    implicit def cycle = new DBInterpreter[Lifecycle] {
      override def effect[T](action: Lifecycle[Free[Lifecycle, T]]): Task[Free[Lifecycle, T]] = ???
      override def toTask: ~>[Lifecycle, Task] = ???
      override def toInstructions[T](exp: Free[Lifecycle, T]): List[String] = ???
      override def toIO: ~>[Lifecycle, IO] = ???
    }

    implicit def default = new DBInterpreter[MongoIO] {
      private def transform[T](op: MongoIO[T]): Task[T] =
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

      override def effect[T](action: MongoIO[Free[MongoIO, T]]): Task[Free[MongoIO, T]] =
        transform(action)

      override def toTask: MongoIO ~> Task = new (MongoIO ~> Task) {
        def apply[T](op: MongoIO[T]) = transform(op)
      }

      override def toIO: MongoIO ~> IO = new (MongoIO ~> IO) {
        def apply[T](op: MongoIO[T]) = op match {
          case FindOne(c, query, next) ⇒ IO {
            val r = c.findOne(query)
            if (r eq null) {
              throw new Exception(s"Can't findOne in $c by $query")
            }
            r
          }.map(next)
          case Insert(c, obj, next) ⇒ IO {
            Try {
              c.insert(WriteConcern.ACKNOWLEDGED, obj)
              obj
            } match {
              case Success(obj)   ⇒ obj
              case Failure(error) ⇒ throw new Exception(s"Can't Insert in $c - $obj", error)
            }
          }.map(next)
        }
      }

      override def toInstructions[T](exp: Free[MongoIO, T]): List[String] = {
        def loop(exp: Free[MongoIO, T], actions: List[String] = Nil): List[String] =
          exp.resume.fold(
            {
              case FindOne(c, query, next) ⇒ loop(next(query), query.toString :: actions)
              case Insert(c, obj, next)    ⇒ loop(next(obj), obj.toString :: actions)

            }, { r: T ⇒ actions.reverse })

        loop(exp)
      }
    }

    def apply[F[_]: Functor](dbName: String, address: InetSocketAddress)(implicit tag: ClassTag[F[_]], delegate: DBInterpreter[F]) =
      new Program[F](dbName, address, delegate, tag.runtimeClass.getCanonicalName)
  }

  final class Program[F[_]] private (dbName: String, address: InetSocketAddress, delegate: DBInterpreter[F], alg: String) {
    private val logger = Logger.getLogger(alg)
    private lazy val client = new MongoClient(new ServerAddress(address))

    type DBFree[T] = Free[F, T]

    def findOne(query: DBObject)(implicit collection: String): DBFree[DBObject] =
      liftF(FindOne(client.getDB(dbName).getCollection(collection), query, identity)).asInstanceOf[DBFree[DBObject]]

    def insert(query: DBObject)(implicit collection: String): DBFree[DBObject] =
      liftF(Insert(client.getDB(dbName).getCollection(collection), query, identity)).asInstanceOf[DBFree[DBObject]]

    /**
     *
     *
     * @return
     */
    def effect[T](action: F[Free[F, T]]): Task[Free[F, T]] = delegate.effect(action)

    /**
     *
     * @return
     */
    def toTask: F ~> Task = delegate.toTask

    /**
     *
     * @return
     */
    def toIO: F ~> IO = delegate.toIO

    /**
     *
     *
     * @return
     */
    def instructions[T](exp: Free[F, T]): List[String] = {
      logger.info(s"Program[$alg] split on instructions")
      delegate.toInstructions(exp)
    }
  }
}