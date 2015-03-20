package doobie

import java.sql.DriverManager

import doobie.util.update.Update0
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import doobie.imports._

import scalaz.Scalaz._
import scalaz._
import scalaz.concurrent.Task

trait Env extends org.specs2.mutable.Before {

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

  "Doobie run transactor less program" in new Env {
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

  "Doobie h2 in memory db" should {
    "create_insert_read" in {
      //:~/test
      //mem:test-db

      val xa = doobie.util.transactor.DriverManagerTransactor[Task](
        "org.h2.Driver",
        "jdbc:h2:mem:test-db;DB_CLOSE_DELAY=-1",
        "sa", "")

      val task = (for {
        _ ← Update0("CREATE TABLE country (code character(3) NOT NULL, name text NOT NULL, population integer NOT NULL)", None).run
        _ ← sql"""INSERT INTO country VALUES ('RUS', 'Russia', 146270)""".update.run
        _ ← sql"""INSERT INTO country VALUES ('USA', 'United States of America', 320480)""".update.run
        r ← sql"SELECT count(*) FROM country".query[Int].unique
      } yield (r)).transact(xa)

      task.attemptRun should be equalTo \/-(2)

      val task0 = sql"SELECT count(*) FROM country".query[Int].unique.transact(xa)
      task0.attemptRun must_== \/-(2)
    }
  }
}
