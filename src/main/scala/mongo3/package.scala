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

  type Dsl[A] = Free.FreeC[MongoIO, A]

  sealed trait MongoIO[+A]
  case class Find[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class FindOne[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class Insert[A](dbName: String, collection: String, insertObj: DBObject) extends MongoIO[A]

  trait Effect[M[_]] {
    protected implicit var exec: ExecutorService = null
    def logger(): Logger
    def withExecutor(ex: ExecutorService): Effect[M] = {
      exec = ex
      this
    }
    def apply[A](a: ⇒ A): M[A]
    def effect[A](client: MongoClient, db: String, c: String, q: DBObject): M[A]
  }

  object Effect {
    def apply[M[_]](implicit sf: Effect[M]): Effect[M] = sf

    @tailrec
    private def loop[A](cursor: DBCursor, logger: Logger, result: Vector[A] = Vector.empty[A]): Vector[A] =
      if (cursor.hasNext) {
        val r = cursor.next.asInstanceOf[A]
        logger.info(s"fetch: $r")
        loop(cursor, logger, result :+ r)
      } else result

    implicit def ObservableAction: Effect[Observable] =
      new Effect[Observable] {
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

    implicit def FutureAction: Effect[Future] =
      new Effect[Future] {
        override val logger = Logger.getLogger("future")
        implicit val EC = scala.concurrent.ExecutionContext.fromExecutor(exec)

        override def apply[A](a: ⇒ A): Future[A] = Future(a)(EC)

        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject): Future[A] =
          Future {
            val cursor = client.getDB(db).getCollection(c).find(q)
            new BasicDBObject("rs", seqAsJavaList(loop(cursor, logger))).asInstanceOf[A]
          }(EC)
      }

    implicit def TaskAction: Effect[Task] =
      new Effect[Task] {
        override val logger = Logger.getLogger("task")

        override def apply[A](a: ⇒ A): Task[A] = Task(a)

        override def effect[A](client: MongoClient, db: String, c: String, q: DBObject) = {
          Task {
            val cursor = client.getDB(db).getCollection(c).find(q)
            new BasicDBObject("rs", seqAsJavaList(loop(cursor, logger))).asInstanceOf[A]
          }(exec)
        }
      }

    type TaskP[x] = Process[Task, x]
    //({ type λ[x] = Process[Task, x] })#λ
    implicit def ProcessAction: Effect[TaskP] =
      new Effect[TaskP] {
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

    implicit def IOAction: Effect[IO] =
      new Effect[IO] {
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

  implicit class MongoInstructionsSyntax[A](val query: Dsl[A]) extends AnyVal {
    import Kleisli._
    type Transformation[M[_]] = MongoIO ~> Kleisli[M, MongoClient, ?]
    //MongoIO ~> ({ type λ[x] = Kleisli[M, MongoClient, x] })#λ

    //val m = implicitly[scalaz.Monad[M]] lookup
    //Monad[Task] and MInstruction[Task]) or
    //Monad[Observer] and MInstruction[Observer] or
    //Monad[IO] and MInstruction[IO] or
    //Monad[Process] and MInstruction[Process]
    def into[M[_]: scalaz.Monad: Effect](implicit ex: ExecutorService): Kleisli[M, MongoClient, A] =
      //runFC[MongoIO, ({ type λ[x] = Kleisli[M, MongoClient, x] })#λ, A](q)(evaluate[M](implicitly[Effect[M]].withExecutor(ex)))
      runFC[MongoIO, Kleisli[M, MongoClient, ?], A](query)(evaluate[M](implicitly[Effect[M]].withExecutor(ex))) //because of kind-projector

    private def evaluate[M[_]](evaluator: Effect[M]) = new Transformation[M] {
      val logger = evaluator.logger

      private def toKleisli[A](f: MongoClient ⇒ A): Kleisli[M, MongoClient, A] =
        kleisli(client ⇒ evaluator(f(client)))

      override def apply[A](fa: MongoIO[A]): Kleisli[M, MongoClient, A] = fa match {
        case FindOne(db, c, q) ⇒ toKleisli { client: MongoClient ⇒
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
        case Find(db, c, q) ⇒ kleisli(client ⇒ evaluator.effect(client, db, c, q))
      }
    }
  }

  trait MongoProgram {
    def dbName: String

    def find(query: DBObject)(implicit collection: String): Dsl[DBObject] =
      Free.liftFC[MongoIO, DBObject](Find(dbName, collection, query))

    def findOne(query: DBObject)(implicit collection: String): Dsl[DBObject] =
      Free.liftFC[MongoIO, DBObject](FindOne(dbName, collection, query))

    def insert(insertObj: DBObject)(implicit collection: String): Dsl[DBObject] =
      Free.liftFC[MongoIO, DBObject](Insert(dbName, collection, insertObj))
  }

  object MongoProgram {
    def apply(db: String) = new MongoProgram {
      override val dbName = db
    }
  }
}