package banking.service

import banking._
import banking.repository.AccountRepository

import scalaz.Kleisli
import scalaz.concurrent.Task

trait ReportingService[Amount] {
  type ReportOperation[A] = Kleisli[Valid, AccountRepository, A]

  /**
   *
   * @return
   */
  def balances: ReportOperation[Seq[(String, Amount)]]
}