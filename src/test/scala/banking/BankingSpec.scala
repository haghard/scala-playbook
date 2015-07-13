package banking

import java.util.Date

import banking.repository.AccountRepo
import org.specs2.mutable.Specification
import scalaz._
import Scalaz._
import Kleisli._

class BankingSpec extends Specification {

  import banking.account._

  import banking.interpreter.AccountService._
  import banking.interpreter.ReportingService._

  def transaction(a: Account, cr: Amount, db: Amount) =
    for {
      _ ← credit(a.no, cr)
      d ← debit(a.no, db)
    } yield d

  def openA() =
    for {
      _ ← open("3445684569463567", "Alan Turing", None, None, Checking)
      _ ← open("3463568456374573", "Nikola Tesla", BigDecimal(456.9).some, None, Savings)
    } yield ()

  def creditA() =
    for {
      _ ← credit("3445684569463567", 5000)
      _ ← credit("3463568456374573", 6000)
    } yield ()

  trait AccountRepositoryInMemory extends AccountRepo {
    lazy val repo = scala.collection.mutable.Map.empty[String, Account]

    override def query(no: String): ValidationNel[String, Option[Account]] =
      repo.get(no).success

    override def store(a: Account): ValidationNel[String, Account] = {
      repo += (a.no -> a)
      a.success
    }

    override def query(openedOn: Date): ValidationNel[String, Seq[Account]] =
      repo.values.filter(_.dateOfOpen == openedOn).toSeq.success

    override def all: ValidationNel[String, Seq[Account]] =
      repo.values.toSeq.success
  }

  object AccountRepositoryFromMap extends AccountRepositoryInMemory

  "Open, credit, transfer, balances" should {
    "run" in {
      val program = for {
        _ ← openA()
        _ ← creditA()
        //a <- balance("a34vzbdfg")
        //b <- balance("a34vzbdfg")
        //_ ← balances
        _ ← transfer("3463568456374573", "3445684569463567", BigDecimal(1000))
        c ← balances
      } yield c

      val r = program(AccountRepositoryFromMap).disjunction

      r.isRight === true
      val seq = r.toOption.get
      seq.foreach(println(_))
      seq.size === 2
    }
  }
}