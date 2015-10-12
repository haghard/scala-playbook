//wait for doobie switch from shapless-2.0.0 to shapless-2.1.0 to work with scalaz-netty
package doobie

import java.sql.DriverManager
import doobie.util.update.Update0
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import doobie.imports._

import scalaz.Scalaz._
import scalaz._
import scalaz.concurrent.Task
import scalaz.effect.IO

trait Environment extends org.specs2.mutable.Before {

  override def before = {
    val xa = doobie.util.transactor.DriverManagerTransactor[Task](
      "org.h2.Driver",
      "jdbc:h2:mem:test-db0;DB_CLOSE_DELAY=-1",
      "sa", "")

    (for {
      _ ← Update0("CREATE TABLE country (code character(3) NOT NULL, name text NOT NULL, population integer NOT NULL)", None).run
      _ ← sql"""INSERT INTO country VALUES ('RUS', 'Russia', 146270)""".update.run
      _ ← sql"""INSERT INTO country VALUES ('USA', 'United States of America', 320480)""".update.run
    } yield ()).transact(xa).attemptRun
  }
}

class DoobieSpec extends Specification with Mockito {
  case class Country(code: String, name: String, population: Int)

  "Doobie" should {
    "run the simplest program" in {
      val xa = doobie.util.transactor.DriverManagerTransactor[Task](
        "org.h2.Driver",
        "jdbc:h2:mem:ch3;DB_CLOSE_DELAY=-1",
        "sa", ""
      )

      val program = 42.point[ConnectionIO]
      val task = program.transact(xa)
      task.run should be equalTo 42
    }
  }

  "Doobie program using existing Connection with Task effect" in new Environment {
    val task: Kleisli[Task, java.sql.Connection, Int] =
      sql"SELECT count(*) FROM country"
        .query[Int]
        .unique
        .transK[Task]

    Class.forName("org.h2.Driver")
    val con = DriverManager.getConnection("jdbc:h2:mem:test-db0;DB_CLOSE_DELAY=-1", "sa", "")
    //mock[java.sql.Connection]
    task.run(con).attemptRun should be equalTo \/-(2)
  }

  "Doobie program using existing Connection with IO effect" in new Environment {
    val task: Kleisli[IO, java.sql.Connection, Int] =
      sql"SELECT count(*) FROM country"
        .query[Int]
        .unique
        .transK[IO]

    Class.forName("org.h2.Driver")
    val con = DriverManager.getConnection("jdbc:h2:mem:test-db0;DB_CLOSE_DELAY=-1", "sa", "")
    //mock[java.sql.Connection]
    task.run(con).unsafePerformIO() should be equalTo 2
  }

  "Doobie program using existing DataSource" in new Environment {
    val ds: javax.sql.DataSource = null
    val xa = DataSourceTransactor.apply[Task](ds)

    val q = sql"SELECT count(*) FROM country".query[Int].unique

    val p: Task[Int] = for {
      _ ← xa.configure(ds ⇒ Task.delay( /* ds.getConnection ..... */ ()))
      a ← q.transact(xa)
    } yield a
  }

  "Doobie h2 in memory db quering" should {
    "quering" in {
      val xa = doobie.util.transactor.DriverManagerTransactor[Task](
        "org.h2.Driver",
        "jdbc:h2:mem:test-db999;DB_CLOSE_DELAY=-1",
        "sa", "")

      (for {
        _ ← Update0("CREATE TABLE country (code character(3) NOT NULL, name text NOT NULL, population integer NOT NULL)", None).run
        _ ← sql"""INSERT INTO country VALUES ('RUS', 'Russia', 146270)""".update.run
        _ ← sql"""INSERT INTO country VALUES ('USA', 'United States of America', 320480)""".update.run
      } yield ()).transact(xa).attemptRun

      val qTask = sql"SELECT count(*) FROM country".query[Int].unique.transact(xa)
      qTask.attemptRun must_== \/-(2)
    }
  }

  val list = Vector(Country("RUS", "Russia", 146270), Country("USA", "United States of America", 320480))

  "Doobie h2 in memory db streaming thought process" in new Environment {
    val xa = doobie.util.transactor.DriverManagerTransactor[Task](
      "org.h2.Driver",
      "jdbc:h2:mem:test-db0;DB_CLOSE_DELAY=-1",
      "sa", "")

    val p = sql"SELECT * FROM country"
      .query[Country]
      .process
      .transact(xa)

    p.runLog.run should be equalTo list
  }

  "Doobie run H2Transactor uses backed JdbcConnectionPool" in new Environment {
    import doobie.imports._, scalaz._, scalaz.concurrent.Task
    import doobie.contrib.h2.h2transactor._

    val list = List(Country("RUS", "Russia", 146270), Country("USA", "United States of America", 320480))

    val q = sql"SELECT * FROM country".query[Country].list

    //The connnection pool has internal state so constructing one is an effect
    (for {
      xa ← H2Transactor[Task]("jdbc:h2:mem:test-db0;DB_CLOSE_DELAY=-1", "sa", "")
      _ ← xa.setMaxConnections(5)
      a ← q.transact(xa).ensuring(xa.dispose)
    } yield a).attemptRun should be equalTo \/-(list)
  }
}
