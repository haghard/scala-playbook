package mongo

import scalaz.Free
import scalaz.concurrent.Task
import mongo.MongoProgram.{ DBFree, MongoAlgebra }

trait Interpreter {

  protected val finalInstruction = "End of program"

  /**
   *
   * @param action
   * @tparam T
   * @return
   */
  def effect[T](action: MongoAlgebra[DBFree[T]]): Task[Free[MongoAlgebra, T]]

  /**
   *
   * @param program
   * @param actions
   * @return
   */
  def instructions[T](program: Free[MongoAlgebra, T], actions: List[String] = Nil): List[String]
}