import java.util.concurrent.ExecutorService

import com.mongodb._
import org.apache.log4j.Logger
import rx.lang.scala.schedulers.ExecutionContextScheduler
import rx.lang.scala.{ Subscriber, Observable }

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }
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

  sealed trait MongoIO[+A]
  case class Find[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class FindOne[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class Insert[A](dbName: String, collection: String, insertObj: DBObject) extends MongoIO[A]

  trait MongoAction[M[_]] {
    protected implicit var exec: ExecutorService = null
    def logger(): Logger
    def withExecutor(ex: ExecutorService): MongoAction[M] = {
      exec = ex
      this
    }
    def apply[A](a: ⇒ A): M[A]
    def effect[A](client: MongoClient, db: String, c: String, q: DBObject): M[A]
  }

  object MongoAction {
    def apply[M[_]](implicit sf: MongoAction[M]): MongoAction[M] = sf

    @tailrec
    private def loop[A](cursor: DBCursor, logger: Logger, result: Vector[A] = Vector.empty[A]): Vector[A] =
      if (cursor.hasNext) {
        val r = cursor.next.asInstanceOf[A]
        logger.info(s"fetch: $r")
        loop(cursor, logger, result :+ r)
      } else result

    implicit def ObservableAction: MongoAction[Observable] =
      new MongoAction[Observable] {
        override val logger = Logger.getLogger("Observable-Producer")
        override def apply[A](a: ⇒ A) = Observable.just(a)
        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject) =
          Observable { subscriber: Subscriber[A] ⇒
            lazy val cursor = (Try {
              Option(client.getDB(db).getCollection(c).find(q))
            } recover {
              case e: Throwable ⇒
                subscriber.onError(e)
                None
            }).get

            subscriber.setProducer(n ⇒ {
              var i = 0
              val c = cursor
              if (c.isDefined) {
                while (i < n && c.get.hasNext && !subscriber.isUnsubscribed) {
                  val r = c.get.next().asInstanceOf[A]
                  logger.info(s"fetch $r")
                  subscriber.onNext(r)
                  i += 1
                }
                subscriber.onCompleted()
              }
            })
          }.subscribeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(exec)))
      }

    implicit def FutureAction: MongoAction[Future] =
      new MongoAction[Future] {
        override val logger = Logger.getLogger("future")
        implicit val EC = scala.concurrent.ExecutionContext.fromExecutor(exec)

        override def apply[A](a: ⇒ A): Future[A] = Future(a)(EC)

        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject): Future[A] =
          Future {
            val cursor = client.getDB(db).getCollection(c).find(q)
            new BasicDBObject("rs", seqAsJavaList(loop(cursor, logger))).asInstanceOf[A]
          }
      }

    implicit def TaskAction: MongoAction[Task] =
      new MongoAction[Task] {
        override val logger = Logger.getLogger("task")

        override def apply[A](a: ⇒ A): Task[A] = Task(a)

        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject) = {
          Task {
            val cursor = client.getDB(db).getCollection(c).find(q)
            new BasicDBObject("rs", seqAsJavaList(loop(cursor, logger))).asInstanceOf[A]
          }
        }
      }

    implicit def ProcessAction: MongoAction[({ type λ[x] = Process[Task, x] })#λ] =
      new MongoAction[({ type λ[x] = Process[Task, x] })#λ] {
        override val logger = Logger.getLogger("Process-Producer")
        override def apply[A](a: ⇒ A) = Process.eval(Task(a))

        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject): Process[Task, A] =
          io.resource(Task(client.getDB(db).getCollection(c).find(q)))(cursor ⇒ Task(cursor.close)) { cursor ⇒
            Task {
              if (cursor.hasNext) {
                val r = cursor.next
                logger.info(s"fetch $r")
                r.asInstanceOf[A]
              } else throw Cause.Terminated(Cause.End)
            }
          }
      }

    implicit def IOAction: MongoAction[IO] =
      new MongoAction[IO] {
        override val logger = Logger.getLogger("io")
        override def apply[A](a: ⇒ A): IO[A] = IO(a)
        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject): IO[A] =
          IO {
            val cursor = client.getDB(db).getCollection(c).find(q)
            new BasicDBObject("rs", seqAsJavaList(loop(cursor, logger))).asInstanceOf[A]
          }
      }
  }

  implicit def observerProgram(implicit ex: ExecutorService) = new scalaz.Monad[Observable]() {
    override def point[A](a: ⇒ A): Observable[A] =
      Observable.defer(Observable.just(a))
        .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(ex)))
    override def bind[A, B](fa: Observable[A])(f: (A) ⇒ Observable[B]) = fa flatMap f
  }

  implicit def futureProgram(implicit ex: ExecutorService) = new scalaz.Monad[scala.concurrent.Future] {
    implicit val EC = scala.concurrent.ExecutionContext.fromExecutor(ex)
    override def point[A](a: ⇒ A): Future[A] = Future(a)
    override def bind[A, B](fa: Future[A])(f: (A) ⇒ Future[B]): Future[B] = fa.flatMap(f)
  }

  implicit class MongoProgramsSyntax[A](val q: FreeMongoIO[A]) extends AnyVal {
    import Kleisli._
    type Transformation[M[_]] = MongoIO ~> ({ type λ[x] = Kleisli[M, MongoClient, x] })#λ

    //val m = implicitly[scalaz.Monad[M]] creates Monad[Task], Monad[Observer], Monad[IO], Monad[Process]
    def transM[M[_]: scalaz.Monad: MongoAction](implicit ex: ExecutorService): Kleisli[M, MongoClient, A] =
      runFC[MongoIO, ({ type λ[x] = Kleisli[M, MongoClient, x] })#λ, A](q)(transformation[M])

    private def transformation[M[_]: scalaz.Monad: MongoAction](implicit ex: ExecutorService) =
      new Transformation[M] {
        val actionInstance = implicitly[MongoAction[M]].withExecutor(ex)
        val logger = actionInstance.logger()

        private def toKleisli[A](f: MongoClient ⇒ A): Kleisli[M, MongoClient, A] =
          kleisli(client ⇒ actionInstance(f(client)))

        override def apply[A](fa: MongoIO[A]): Kleisli[M, MongoClient, A] = fa match {
          case FindOne(db, c, q) ⇒ toKleisli { client ⇒
            val coll = client.getDB(db).getCollection(c)
            val r = coll.findOne(q)
            logger.info(s"Find-One: $r")
            new BasicDBObject("rs", r).asInstanceOf[A]
          }
          case Insert(db, c, insert) ⇒ toKleisli { client ⇒
            Try {
              val coll = client.getDB(db).getCollection(c)
              coll.insert(WriteConcern.ACKNOWLEDGED, insert)
              logger.info(s"Insert: $insert")
              insert
            } match {
              case Success(obj)   ⇒ obj.asInstanceOf[A]
              case Failure(error) ⇒ throw new MongoException(s"Failed to insert in $c object: ${insert}", error)
            }
          }
          //batching or streaming
          case Find(db, c, q) ⇒ kleisli(client ⇒ actionInstance.effect(client, db, c, q))
        }
      }
  }

  trait Mprogram {
    def dbName: String

    def find(query: DBObject)(implicit collection: String): FreeMongoIO[DBObject] =
      Free.liftFC[MongoIO, DBObject](Find(dbName, collection, query))

    def findOne(query: DBObject)(implicit collection: String): FreeMongoIO[DBObject] =
      Free.liftFC[MongoIO, DBObject](FindOne(dbName, collection, query))

    def insert(insertObj: DBObject)(implicit collection: String): FreeMongoIO[DBObject] =
      Free.liftFC[MongoIO, DBObject](Insert(dbName, collection, insertObj))
  }

  object Mprogram {
    def apply(db: String) = new Mprogram {
      override val dbName = db
    }
  }
}