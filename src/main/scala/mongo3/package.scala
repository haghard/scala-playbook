import java.util.concurrent.ExecutorService

import com.mongodb._
import org.apache.log4j.Logger

import scala.util.{ Failure, Success, Try }
import scalaz.concurrent.Task
import scalaz._
import Free._
import scala.collection.JavaConversions._
import scalaz.effect.IO
import scala.language.higherKinds

package object mongo3 {
  import scalaz.stream._

  type FreeMongoIO[A] = Free.FreeC[MongoIO, A]
  type KleisliDelegate[A] = Kleisli[Task, MongoClient, A]

  sealed trait MongoIO[+A]
  case class FindOne[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class Find[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class Insert[A](dbName: String, collection: String, insertObj: DBObject) extends MongoIO[A]

  trait SideEffects[M[_]] { self ⇒
    protected implicit var exec: ExecutorService = null
    def withExecutor(ex: ExecutorService): SideEffects[M] = {
      exec = ex
      this
    }

    def apply[A](a: ⇒ A): M[A]
    def withResource[A](client: MongoClient, db: String, c: String, q: DBObject): M[A]
  }

  object SideEffects {
    def apply[M[_]](implicit sf: SideEffects[M]): SideEffects[M] = sf

    implicit val TaskCapture: SideEffects[Task] =
      new SideEffects[Task] {
        override def apply[A](a: ⇒ A): Task[A] = Task(a)

        override def withResource[A](client: MongoClient, db: String, c: String, q: DBObject): Task[A] =
          (io.resource(Task(client.getDB(db).getCollection(c).find(q)))(cursor ⇒ Task(cursor.close)) { cursor ⇒
            Task {
              if (cursor.hasNext) {
                val r = cursor.next.asInstanceOf[A]
                println(s"${Thread.currentThread().getName} - fetch: $r")
                r
              } else throw Cause.Terminated(Cause.End)
            }
          }).runLog.map(res ⇒ new BasicDBObject("rs", seqAsJavaList(res)).asInstanceOf[A])
      }

    implicit val IOContext: SideEffects[IO] =
      new SideEffects[IO] {
        override def apply[A](a: ⇒ A): IO[A] = IO(a)

        override def withResource[A](client: MongoClient, db: String, c: String, q: DBObject): IO[A] =
          for {
            cursor ← IO(client.getDB(db).getCollection(c).find(q))
          } yield {
            val r = new BasicDBObject("rs", seqAsJavaList(loop(cursor, Nil))).asInstanceOf[A]
            cursor.close()
            r
          }

        private def loop[A](cursor: DBCursor, list: List[A]): List[A] =
          if (cursor.hasNext) {
            val r = cursor.next.asInstanceOf[A]
            println(s"fetch: $r")
            loop(cursor, r :: list)
          } else list
      }
  }

  implicit class MongoConnectionIOops[A](q: FreeMongoIO[A]) {
    private val logger = Logger.getLogger(classOf[ProgramOps])

    def transM[M[_]: Monad: SideEffects](implicit ex: ExecutorService): Kleisli[M, MongoClient, A] =
      runFC[MongoIO, ({ type f[x] = Kleisli[M, MongoClient, x] })#f, A](q)(kleisliTrans[M])

    private def kleisliTrans[M[_]: Monad: SideEffects](implicit ex: ExecutorService): MongoIO ~> ({ type f[x] = Kleisli[M, MongoClient, x] })#f =
      new (MongoIO ~>({ type f[x] = Kleisli[M, MongoClient, x] })#f) {
        private val targetMonad = implicitly[SideEffects[M]].withExecutor(ex)

        private def toKleisli[A](f: MongoClient ⇒ A): Kleisli[M, MongoClient, A] =
          Kleisli.kleisli { s ⇒ targetMonad(f(s)) }

        override def apply[A](fa: MongoIO[A]): Kleisli[M, MongoClient, A] = fa match {
          case FindOne(db, c, q) ⇒ toKleisli {
            client ⇒
              {
                val coll = client.getDB(db).getCollection(c)
                val r = coll.findOne(q)
                logger.info(s"FIND-ONE: $r")
                new BasicDBObject("rs", r).asInstanceOf[A]
              }
          }
          case Find(db, c, q) ⇒ Kleisli.kleisli(targetMonad.withResource(_, db, c, q))
          case Insert(db, c, insert) ⇒ toKleisli { client ⇒
            Try {
              val coll = client.getDB(db).getCollection(c)
              coll.insert(WriteConcern.ACKNOWLEDGED, insert)
              logger.info(s"INSERT: $insert")
              insert
            } match {
              case Success(obj)   ⇒ obj.asInstanceOf[A]
              case Failure(error) ⇒ throw new Exception(s"Can't Insert in $c - ${insert}", error)
            }
          }
        }
      }
  }

  trait ProgramOps {
    def dbName: String

    def find(query: DBObject)(implicit collection: String) =
      Free.liftFC[MongoIO, DBObject](Find(dbName, collection, query))

    def findOne(query: DBObject)(implicit collection: String) =
      Free.liftFC[MongoIO, DBObject](FindOne(dbName, collection, query))

    def insert(insertObj: DBObject)(implicit collection: String) =
      Free.liftFC[MongoIO, DBObject](Insert(dbName, collection, insertObj))
  }

  object ProgramOps {
    def apply(db: String) = new ProgramOps {
      override val dbName = db
    }
  }
}