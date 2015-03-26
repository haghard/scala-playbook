package mongo

import java.util.concurrent.Executors
import de.bwaldvogel.mongo.MongoServer
import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import com.mongodb.{ ServerAddress, MongoClient, DBObject, BasicDBObject }

import mongo3._
import scalaz.effect.IO
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task

trait EnviromentBefore extends org.specs2.mutable.Before {
  var server: MongoServer = null
  var client: MongoClient = null

  override def before = {
    server = new MongoServer(new MemoryBackend())
    client = new MongoClient(new ServerAddress(server.bind()))
  }
}

class Mongo3AlgSpec extends Specification {

  implicit val collection = "testCollection"
  implicit val executor =
    Executors.newFixedThreadPool(1, new NamedThreadFactory("mongo-worker"))

  val data = new BasicDBObject().append("catId", 512).append("name", "Gardening Tools")
  val data1 = new BasicDBObject().append("catId", 513).append("name", "Gardening Tools")

  "mongo3.Program fetch single result with Task" in new EnviromentBefore {
    def query(id: Int) = new BasicDBObject("catId", id)

    val program = ProgramOps(collection)
    val exp = for {
      id ← program.insert(data)
      rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))
    } yield rs

    val task = exp.transM[Task].run(client)

    val r = task.attemptRun match {
      case \/-(obj) ⇒ obj.get("rs").asInstanceOf[DBObject] ne null
      case -\/(err) ⇒ false
    }

    r should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch single result with IO" in new EnviromentBefore {
    def query(id: Int) = new BasicDBObject("catId", id)

    val program = ProgramOps(collection)
    val exp = for {
      id ← program.insert(data)
      rs ← program.findOne(query(id.get("catId").asInstanceOf[Int]))
    } yield rs

    val ioTask = exp.transM[IO].run(client)
    val r = ioTask.unsafePerformIO()

    r != null should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch multi results with Task" in new EnviromentBefore {
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gt", id))

    val program = ProgramOps(collection)

    val exp = for {
      _ ← program.insert(data)
      _ ← program.insert(data1)
      r ← program.find(query(500))
    } yield r

    val task = exp.transM[Task].run(client)

    val r = task.attemptRun match {
      case \/-(obj)   ⇒ obj.get("rs").asInstanceOf[java.util.List[DBObject]].size() == 2
      case -\/(error) ⇒ false
    }
    r should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch multi results with IO" in new EnviromentBefore {
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gt", id))

    val program = ProgramOps(collection)

    val exp = for {
      _ ← program.insert(data)
      _ ← program.insert(data1)
      r ← program.find(query(500))
    } yield r

    val ioTask = exp.transM[IO].run(client)
    val r = ioTask.unsafePerformIO()

    r != null should beTrue
    client.close()
    server.shutdownNow
  }
}