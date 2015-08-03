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

  def open2(): Kleisli[Valid, AccountRepo, (Balance, Balance)] =
    for {
      a0 ← open("3445684569463567", "Alan Turing", None, None, Checking)
      a1 ← open("3463568456374573", "Nikola Tesla", BigDecimal(456.9).some, None, Savings)
    } yield (a0.balance, a1.balance)

  def credit2(): Kleisli[Valid, AccountRepo, (String, String)] =
    for {
      a0 ← credit("3445684569463567", 5000)
      a1 ← credit("3463568456374573", 6000)
    } yield (a0.no, a1.no)

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
        _ ← open2()
        nums ← credit2()
        //a <- balance("a34vzbdfg")
        //b <- balance("a34vzbdfg")
        //_ ← balances
        _ = println(nums._1 + " - " + nums._2)
        _ ← transfer(nums._1, nums._2, BigDecimal(1000))
        c ← balances
      } yield c

      val r = program(AccountRepositoryFromMap).disjunction

      r.isRight === true
      val seq = r.toOption.get
      seq.foreach(println(_))
      seq.size === 2
    }
  }

  "Balances" should {
    "have streamed through process api" in {
      val Repo = AccountRepositoryFromMap
      val program = for { _ ← open2() } yield ()

      program(Repo)
      val res: Valid[Seq[(String, Amount)]] = balancesProcess.run(Repo).runLog.run(0)
      res.isSuccess === true
      res.toOption.get.size == 2
    }
  }
}