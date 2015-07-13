package banking.interpreter

import banking._
import banking.account.Account
import banking.repository.AccountRepository
import banking.service.ReportingService
import scalaz.Kleisli._
import scalaz._
import Scalaz._

trait ReportingServiceInterpreter extends ReportingService[Amount] {
  override def balances: ReportOperation[Seq[(String, Amount)]] =
    kleisli[Valid, AccountRepository, Seq[(String, Amount)]] { (repo: AccountRepository) ⇒
      repo.all.fold({ error ⇒ error.toString().failureNel }, { as: Seq[Account] ⇒
        as.map(a ⇒ (a.no, a.balance.amount)).success
      })
    }
}

object ReportingService extends ReportingServiceInterpreter