import java.util.concurrent.ExecutorService

import com.mongodb._
import org.apache.log4j.Logger

import scala.util.{ Failure, Success, Try }
import scalaz.concurrent.Task
import scalaz._
import Free._
import scala.collection.JavaConversions._

package object mongo3 {
  import scalaz.stream._

  type FreeMongoIO[A] = Free.FreeC[MongoIO, A]
  type KleisliDelegate[A] = Kleisli[Task, MongoClient, A]

  sealed trait MongoIO[+A]
  case class FindOne[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class Find[A](dbName: String, collection: String, query: DBObject) extends MongoIO[A]
  case class Insert[A](dbName: String, collection: String, insertObj: DBObject) extends MongoIO[A]

  implicit class MongoConnectionIOops[A](q: FreeMongoIO[A]) {
    private val logger = Logger.getLogger(classOf[ProgramOps])

    def kleisli(implicit ex: ExecutorService): KleisliDelegate[A] = runFC[MongoIO, KleisliDelegate, A](q)(~>)

    def ~>(implicit ex: ExecutorService): MongoIO ~> KleisliDelegate = {
      new (MongoIO ~> KleisliDelegate) {
        override def apply[T](op: MongoIO[T]): KleisliDelegate[T] = {
          op match {
            case FindOne(db, c, q) ⇒ Kleisli.kleisli(client ⇒ Task {
              val coll = client.getDB(db).getCollection(c)
              val r = coll.findOne(q)
              logger.info(s"FIND-ONE: $r")
              if (r == null) throw new Exception(s"Can't findOne in $c by $q")
              new BasicDBObject("rs", r).asInstanceOf[T]
            })
            case Find(db, c, q) ⇒ Kleisli.kleisli { client ⇒
              (io.resource(Task(client.getDB(db).getCollection(c).find(q)))(cursor ⇒ Task(cursor.close)) { cursor ⇒
                Task {
                  if (cursor.hasNext) {
                    val r = cursor.next.asInstanceOf[T]
                    if (r == null) throw new Exception(s"Can't find in $c by $q")
                    logger.info(s"fetch: $r")
                    r
                  } else throw Cause.Terminated(Cause.End)
                }
              }).runLog.flatMap(res ⇒ Task(new BasicDBObject("rs", seqAsJavaList(res)).asInstanceOf[T]))
            }
            case Insert(db, c, insert) ⇒ Kleisli.kleisli(client ⇒ Task {
              Try {
                val coll = client.getDB(db).getCollection(c)
                coll.insert(WriteConcern.ACKNOWLEDGED, insert)
                logger.info(s"INSERT: $insert")
                insert
              } match {
                case Success(obj)   ⇒ obj.asInstanceOf[T]
                case Failure(error) ⇒ throw new Exception(s"Can't Insert in $c - ${insert}", error)
              }
            })
          }
        }
      }
    }
  }

  trait ProgramOps {
    def dbName: String

    def find(query: DBObject)(implicit collection: String): FreeMongoIO[DBObject] =
      Free.liftFC[MongoIO, DBObject](Find(dbName, collection, query))

    def findOne(query: DBObject)(implicit collection: String): FreeMongoIO[DBObject] =
      Free.liftFC[MongoIO, DBObject](FindOne(dbName, collection, query))

    def insert(insertObj: DBObject)(implicit collection: String): FreeMongoIO[DBObject] =
      Free.liftFC[MongoIO, DBObject](Insert(dbName, collection, insertObj))
  }

  object ProgramOps {
    def apply(db: String) = new ProgramOps {
      override val dbName = db
    }
  }
}