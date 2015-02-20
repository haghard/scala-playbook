package mongo

import java.util.concurrent.Executors

import com.mongodb.WriteConcern
import mongo.MongoProgram._
import org.apache.log4j.Logger

import scala.util.{ Failure, Success, Try }
import scalaz.Free
import scalaz.concurrent.Task

trait JavaMongoDriverInterpreter extends Interpreter {

  private val logger = Logger.getLogger(classOf[JavaMongoDriverInterpreter])
  /**
   *
   * @param action
   * @tparam T
   * @return
   */
  override def effect[T](action: MongoAlgebra[DBFree[T]]): Task[Free[MongoAlgebra, T]] =
    action match {
      case Insert(collection, obj, next) ⇒
        Task {
          Try {
            logger info (s"Insert: $obj into $collection")
            collection insert (WriteConcern.ACKNOWLEDGED, obj)
            obj
          } match {
            case Success(obj)   ⇒ obj
            case Failure(error) ⇒ throw new Exception(s"Can't Insert in $collection - $obj", error)
          }
        } map (next)
      case FindOne(collection, query, next) ⇒
        Task {
          logger info (s"Find: $query in $collection")
          Option(collection.findOne(query))
            .fold {
              throw new Exception(s"Can't findOne in $collection by $query")
            } { r ⇒ r }
        } map (next)
      case Fail(cause) ⇒ Task.fail(cause)
    }

  /**
   *
   * @param program
   * @param actions
   * @tparam T
   * @return
   */
  override def instructions[T](program: DBFree[T], actions: List[String] = Nil): List[String] =
    program.resume.fold(
      {
        case Insert(coll, obj, next)    ⇒ instructions(next(obj), s"Insert: $obj into $coll" :: actions)
        case FindOne(coll, query, next) ⇒ instructions(next(query), s"Find: $query in $coll" :: actions)
        case Fail(cause)                ⇒ (s"Fail: ${cause.getMessage}" :: actions).reverse
      }, { r: T ⇒ (finalInstruction :: actions).reverse })
}

object JavaMongoDriverInterpreter {

  implicit val mongoExecutor =
    Executors.newFixedThreadPool(5, new NamedThreadFactory("java-mongo-worker"))

  implicit val javaMongo = new InterpreterOps[JavaMongoDriverInterpreter] {
    override def apply() = new JavaMongoDriverInterpreter {}
  }
}
