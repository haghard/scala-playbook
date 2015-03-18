package mongo

import mongo2.MongoIO
import scalaz.effect.IO
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task
import java.net.InetSocketAddress
import de.bwaldvogel.mongo.MongoServer
import org.specs2.mutable.Specification
import com.mongodb.{ DBObject, BasicDBObject }
import de.bwaldvogel.mongo.backend.memory.MemoryBackend

class Mongo2AlgSpec extends Specification {
  implicit val collection = "testCollection"
  val collection1 = "testCollection2"

  private def query(id: Int) = new BasicDBObject("catId", id)

  "mongo2.Program" should {
    "execute 2 program with natural transformation into IO" in {
      val errorIO = IO(println("Operation error"))
      val server = new MongoServer(new MemoryBackend())
      val program = mongo2.Program[MongoIO](collection, server.bind())
      val data = new BasicDBObject().append("catId", 299).append("name", "Gardening Tools")

      val exp0 = (for {
        id ← program.insert(data)
        rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))
      } yield rs).foldMap(program.toIO)

      val exp1 = (for {
        id ← program.insert(data)(collection1)
        rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))(collection1)
      } yield rs).foldMap(program.toIO)

      val result = (exp0 unsafeZip exp1)
        .onException(errorIO)
        .unsafePerformIO()

      server.shutdownNow
      result._1 should be equalTo result._2
    }
  }

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