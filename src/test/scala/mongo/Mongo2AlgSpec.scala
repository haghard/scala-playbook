package mongo

import java.net.InetSocketAddress

import com.mongodb.{ DBObject, BasicDBObject }
import de.bwaldvogel.mongo.MongoServer
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import mongo2.MongoIO
import org.specs2.mutable.Specification
import scalaz.effect.IO
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task

class Mongo2AlgSpec extends Specification {
  implicit val collection = "testCollection"
  private def query(id: Int) = new BasicDBObject("catId", id)

  "mongo2.Program" should {
    "execute with natural transformation into Task" in {
      val server = new MongoServer(new MemoryBackend())
      val program = mongo2.Program[MongoIO](collection, server.bind())
      val data = new BasicDBObject().append("catId", 299).append("name", "Gardening Tools")
      val exp = (for {
        id ← program.insert(data)
        rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))
      } yield rs)

      val task: Task[DBObject] = exp.foldMap(program.toTask)
      val r = task.attemptRun match {
        case \/-(obj)   ⇒ true
        case -\/(error) ⇒ println(error); false
      }
      server.shutdownNow
      r should beTrue
    }
  }

  "mongo2.Program" should {
    "execute with natural transformation into IO" in {
      val errorIO = IO(println("Operation error"))
      val server = new MongoServer(new MemoryBackend())
      val program = mongo2.Program[MongoIO](collection, server.bind())
      val data = new BasicDBObject().append("catId", 299).append("name", "Gardening Tools")
      val exp = (for {
        id ← program.insert(data)
        rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))
      } yield rs)

      val ioAction = exp.foldMap(program.toIO)
      val result = ioAction.onException(errorIO).unsafePerformIO()

      server.shutdownNow
      result.get("catId").asInstanceOf[Int] should be equalTo 299
      //true should beTrue
    }
  }

  "mongo2.Program" should {
    "execute with effect" in {
      val server = new MongoServer(new MemoryBackend())
      val program = mongo2.Program[MongoIO](collection, server.bind())
      val data = new BasicDBObject().append("catId", 399).append("name", "Gardening Tools")
      val exp = (for {
        id ← program.insert(data)
        rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))
      } yield rs)

      val task: Task[DBObject] = exp.runM(program.effect)
      val r = task.attemptRun match {
        case \/-(obj)   ⇒ true
        case -\/(error) ⇒ println(error); false
      }

      server.shutdownNow
      r should beTrue
    }
  }

  "mongo2.Program" should {
    "split on instructions" in {
      val program = mongo2.Program[MongoIO](collection, new InetSocketAddress(8000))
      val insertQ = new BasicDBObject().append("catId", 399).append("name", "Gardening Tools")
      val findQ = query(399)

      val exp = (for {
        id ← program.insert(insertQ)
        rs ← program.findOne(findQ)
      } yield rs)

      program.instructions(exp) should be equalTo List(insertQ.toString, findQ.toString)
    }
  }
}