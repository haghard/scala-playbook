package banking.repository

import java.util.Date

import banking._
import banking.account._

import scalaz._
import Scalaz._

trait AccountRepo {
  /**
   *
   * @param no
   * @return
   */
  def query(no: String): ValidationNel[String, Option[Account]]

  /**
   *
   * @param a
   * @return
   */
  def store(a: Account): ValidationNel[String, Account]

  /**
   *
   * @param no
   * @return
   */
  def balance(no: String): ValidationNel[String, Balance] =
    query(no).fold(
      { error ⇒ s"No account exists with no $no".failureNel[Balance] }, { a: Option[Account] ⇒
        a.fold("No account exists with no $no".failureNel[Balance]) { r ⇒
          r.balance.success
        }
      })

  /**
   *
   * @param openedOn
   * @return
   */
  def query(openedOn: Date): ValidationNel[String, Seq[Account]]

  /**
   *
   * @return
   */
  def all: ValidationNel[String, Seq[Account]]
}