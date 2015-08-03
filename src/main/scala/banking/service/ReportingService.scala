package banking.service

import banking._
import banking.repository.AccountRepo

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

trait ReportingService[Amount] {
  type ReportOperation[A] = Kleisli[Valid, AccountRepo, A]

  /**
   *
   * @return
   */
  def balances: ReportOperation[Seq[(String, Amount)]]

  /**
   *
   * @return
   */
  def balancesProcess: Reader[AccountRepo, Process[Task, Valid[Seq[(String, Amount)]]]]
}