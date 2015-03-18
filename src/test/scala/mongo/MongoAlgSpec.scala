package mongo

import com.mongodb.{ BasicDBObject, DBObject }
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import mongo.MongoProgram._
import org.specs2.mutable.Specification

import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task

class MongoAlgSpec extends Specification {
  implicit val collection = "testCollection"
  private def query(id: Int) = new BasicDBObject("catId", id)

  "Multi steps mongo program" should {
    "be interpreted" in {
      val server = new MongoServer(new MemoryBackend())
      withTestMongoServer[JavaMongoDriverInterpreter](server) { db ⇒
        val data = new BasicDBObject().append("catId", 12).append("name", "Gardening Tools")
        val program =
          for {
            id ← db.insert(data)
            rs ← db.findOne(query(id.get("catId").asInstanceOf[Int]))
          } yield (rs)
        assert(db.instructions(program).size > 0)
      }
      true should be equalTo true
    }
  }

  "MongoProgram" should {
    "execute with success" in {
      val server = new MongoServer(new MemoryBackend())
      withTestMongoServer[JavaMongoDriverInterpreter](server) { db ⇒
        val data = new BasicDBObject().append("catId", 11).append("name", "Gardening Tools")
        val program: DBFree[DBObject] =
          (for {
            id ← db.insert(data)
            rs ← db.findOne(query(id.get("catId").asInstanceOf[Int]))
          } yield rs)

        val task: Task[DBObject] = program.runM(db.effect)
        val result = task.attemptRun match {
          case \/-(obj)   ⇒ true
          case -\/(error) ⇒ println(error); false
        }
        result should ===(true)
      }
      true should be equalTo true
    }
  }

  "MongoProgram" should {
    "crash on findOne execution" in {
      val server = new MongoServer(new MemoryBackend())
      withTestMongoServer[JavaMongoDriverInterpreter](server) { db ⇒
        val data = new BasicDBObject().append("catId", 99).append("name", "Gardening Tools")
        val program: DBFree[DBObject] =
          (for {
            id ← db.insert(data)
            rs ← db.findOne(query(id.get("catId").asInstanceOf[Int] + 1))
          } yield rs)

        val task: Task[DBObject] = program.runM(db.effect)

        val result = task.attemptRun match {
          case \/-(obj)   ⇒ false
          case -\/(error) ⇒ true
        }
        result should ===(true)
      }
      true should be equalTo true
    }
  }
}