package banking.interpreter

import scalaz._
import Scalaz._
import banking._
import scalaz.Kleisli._
import banking.account.Account
import banking.repository.AccountRepo
import banking.service.ReportingService
import scalaz.concurrent.Task
import scalaz.stream.Process

trait ReportingServiceInterpreter extends ReportingService[Amount] {
  private val P = Process

  override def balances: ReportOperation[Seq[(String, Amount)]] =
    kleisli[Valid, AccountRepo, Seq[(String, Amount)]] { (repo: AccountRepo) ⇒
      repo.all.fold({ error ⇒ error.toString().failureNel }, { as: Seq[Account] ⇒
        as.map(a ⇒ (a.no, a.balance.amount)).success
      })
    }

  override def balancesProcess: Reader[AccountRepo, Process[Task, Valid[Seq[(String, Amount)]]]] =
    Reader { repo ⇒
      P.eval(Task.delay(repo)).map(balances.run(_))
        .onFailure { ex: Throwable ⇒
          P.eval(Task.delay(ex.getMessage.failureNel))
        }
    }
}

object ReportingService extends ReportingServiceInterpreter