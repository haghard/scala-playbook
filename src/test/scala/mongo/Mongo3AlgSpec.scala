package mongo

import mongo3._
import com.mongodb._
import java.util.concurrent.Executors._
import java.util.concurrent.TimeUnit
import com.twitter.util.CountDownLatch
import de.bwaldvogel.mongo.MongoServer
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import de.bwaldvogel.mongo.backend.memory.MemoryBackend
import java.util.concurrent.atomic.AtomicLong

import rx.lang.scala.Observable
import rx.lang.scala.schedulers.ExecutionContextScheduler
import scala.collection.mutable.Buffer
import scala.concurrent.{ ExecutionContext, Await, Future }
import scalaz.effect.IO
import scalaz.stream.{ io, Process }
import scalaz.{ -\/, \/- }
import scalaz.concurrent.Task
import scala.concurrent.duration.FiniteDuration

trait MongoEnviroment extends org.specs2.mutable.Before {
  var server: MongoServer = null
  var client: MongoClient = null

  override def before = {
    server = new MongoServer(new MemoryBackend())
    client = new MongoClient(new ServerAddress(server.bind()))
  }
}

class Mongo3AlgSpec extends Specification {
  implicit val collection = "testCollection"
  implicit val executor = newFixedThreadPool(4, new NamedThreadFactory("mongo-action"))

  val dynamic = new BasicDBObject().append("catId", 512).append("name", "Gardening Tools")
  val data1 = new BasicDBObject().append("catId", 513).append("name", "Gardening Tools1")
  val data2 = new BasicDBObject().append("catId", 514).append("name", "Gardening Tools2")
  val data3 = new BasicDBObject().append("catId", 515).append("name", "Gardening Tools3")

  def dynamic(i: Int) = new BasicDBObject().append("catId", i).append("name", "Gardening Tools" + i)

  "mongo3.Program fetch single result with Task" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", id)

    val program = MongoInstructions(collection)
    val exp = for {
      id ← (program insert dynamic)
      rs ← (program findOne query(id.get("catId").asInstanceOf[Int]))
    } yield rs

    val task = exp.into[Task].run(client)

    val r = task.attemptRun match {
      case \/-(obj) ⇒ obj.get("rs").asInstanceOf[DBObject] ne null
      case -\/(err) ⇒ false
    }

    r should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch single result with IO" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", id)

    val program = MongoInstructions(collection)
    val exp = for {
      id ← (program insert dynamic)
      rs ← (program findOne query(id.get("catId").asInstanceOf[Int]))
    } yield rs

    val ioTask = exp.into[IO].run(client)
    val r = ioTask.unsafePerformIO()

    r != null should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch single result with Future" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", id)

    val program = MongoInstructions(collection)
    val exp = for {
      id ← program insert dynamic
      rs ← (program findOne query(id.get("catId").asInstanceOf[Int]))
    } yield rs

    val dbFuture = exp.into[Future].run(client)
    val r = Await.ready(dbFuture, new FiniteDuration(5, TimeUnit.SECONDS))

    r != null should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch single result with Observable" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", id)
    val program = MongoInstructions(collection)

    val exp = for {
      id ← program insert dynamic
      rs ← program findOne query(id.get("catId").asInstanceOf[Int])
    } yield rs

    val r = exp.into[Observable].run(client).toBlocking.first

    r.get("rs").asInstanceOf[DBObject].get("catId").asInstanceOf[Int] === 512
    client.close()
    server.shutdownNow
  }

  "mongo3.Program fetch single result with Process" in new MongoEnviroment {
    type PTask[X] = Process[Task, X]

    def query(id: Int) = new BasicDBObject("catId", id)
    val program = MongoInstructions(collection)

    val exp = for {
      id ← program insert dynamic
      rs ← program findOne query(id.get("catId").asInstanceOf[Int])
    } yield rs

    val r = exp.into[PTask].run(client).runLog.run

    r(0).get("rs").asInstanceOf[DBObject].get("catId").asInstanceOf[Int] === 512
    client.close()
    server.shutdownNow
  }

  "mongo3.Program streaming with Process" in new MongoEnviroment {
    type PTask[X] = Process[Task, X]
    val Size = 50
    val logger = Logger.getLogger("Process-Consumer")
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gte", id))

    val latch = new CountDownLatch(1)
    val program = MongoInstructions("testDB")

    def insert(i: Int) = for {
      _ ← program.insert(dynamic(600 + i))
    } yield ()

    (0 until Size).foreach(i ⇒ insert(i).into[Task].run(client).run)

    val buf = Buffer.empty[DBObject]
    val BufferSink = io.fillBuffer(buf)
    val LoggerSink = scalaz.stream.sink.lift[Task, DBObject](obj ⇒ Task.delay(logger.info(obj)))

    Task.fork {
      ((for (r ← (program.find(query(600)))) yield r)
        .into[PTask].run(client) observe (LoggerSink) to BufferSink)
        .run[Task]
    }(executor).runAsync(_ ⇒ latch.countDown())

    latch.await()
    buf.size === Size
    client.close()
    server.shutdownNow
  }

  "mongo3.Program streaming with Observable" in new MongoEnviroment {
    val Size = 150
    val logger = Logger.getLogger("Observable-Consumer")
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gt", id))
    val program = MongoInstructions("testDB")

    def insert(i: Int) = for {
      _ ← program.insert(dynamic(600 + i))
    } yield ()

    (0 until Size).foreach(i ⇒ insert(i).into[Task].run(client).run)

    val latch = new CountDownLatch(1)
    val counter = new AtomicLong(0)
    val s = new rx.lang.scala.Subscriber[DBObject] {
      val batchSize = 7
      override def onStart() = request(batchSize)
      override def onNext(n: DBObject) = {
        logger.info(s"$n")
        if (counter.incrementAndGet() % batchSize == 0) {
          logger.info(s"★ ★ ★ ★ ★ ★   Page:[$batchSize]  ★ ★ ★ ★ ★ ★ ")
          request(batchSize)
        }
      }
      override def onError(e: Throwable) = latch.countDown()
      override def onCompleted() = latch.countDown()
    }

    (for { r ← (program find query(500)) } yield r)
      .into[Observable].run(client)
      .observeOn(ExecutionContextScheduler(ExecutionContext.fromExecutor(executor)))
      .subscribe(s)

    latch.await()
    counter.get() === Size
    client.close()
    server.shutdownNow
  }

  "mongo3.Program batching with Task" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gt", id))

    val program = MongoInstructions(collection)

    val exp = for {
      _ ← program insert dynamic
      _ ← program insert data1
      r ← program find query(500)
    } yield r

    val task = exp.into[Task].run(client)

    val r = task.attemptRun match {
      case \/-(obj)   ⇒ obj.get("rs").asInstanceOf[java.util.List[DBObject]].size() == 2
      case -\/(error) ⇒ false
    }

    r should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program batching with IO" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gt", id))

    val program = MongoInstructions(collection)

    val exp = for {
      _ ← program insert dynamic
      _ ← program insert data1
      r ← program find query(500)
    } yield r

    val ioTask = exp.into[IO].run(client)
    val r = ioTask.unsafePerformIO()

    r != null should beTrue
    client.close()
    server.shutdownNow
  }

  "mongo3.Program batching results with Future" in new MongoEnviroment {
    def query(id: Int) = new BasicDBObject("catId", new BasicDBObject("$gt", id))

    val program = MongoInstructions(collection)

    val exp = for {
      _ ← program insert dynamic
      _ ← program insert data1
      r ← program find query(500)
    } yield r

    val ioFuture = exp.into[Future].run(client)
    val r = Await.result(ioFuture, new FiniteDuration(5, TimeUnit.SECONDS))
    r.get("rs").asInstanceOf[java.util.List[DBObject]].size() === 2
    client.close()
    server.shutdownNow
  }
}