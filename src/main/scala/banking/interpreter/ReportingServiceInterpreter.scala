package banking.interpreter

import banking._
import banking.account.Account
import banking.repository.AccountRepo
import banking.service.ReportingService
import scalaz.Kleisli._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

trait ReportingServiceInterpreter extends ReportingService[Amount] {
  type Reporting = ReportOperation[Seq[(String, Amount)]]
  val P = Process

  override def balances: Reporting =
    kleisli[Valid, AccountRepo, Seq[(String, Amount)]] { (repo: AccountRepo) ⇒
      repo.all.fold({ error ⇒ error.toString().failureNel }, { as: Seq[Account] ⇒
        as.map(a ⇒ (a.no, a.balance.amount)).success
      })
    }

  override def balancesP: Reader[AccountRepo, Process[Task, Valid[Seq[(String, Amount)]]]] =
    Reader { repo ⇒
      P.eval(Task.delay(repo)).map(balances.run(_))
        .onFailure { ex: Throwable ⇒
          P.eval(Task.delay(ex.getMessage.failureNel))
        }
    }
}

object ReportingService extends ReportingServiceInterpreter