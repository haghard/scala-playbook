package banking
package service

import banking.account.AccountType
import banking.repository.AccountRepo

import scalaz._
import Scalaz._
import java.util.Date
import scalaz.{ Bind, Kleisli }

trait AccountService[Account, Amount, Balance] {
  type AccountOperation[A] = Kleisli[Valid, AccountRepo, A]

  implicit val B = new Bind[Valid] {
    override def bind[A, B](fa: Valid[A])(f: (A) ⇒ Valid[B]): Valid[B] =
      fa.fold({ error ⇒ error.toString().failureNel }, { res ⇒ f(res) })

    override def map[A, B](fa: Valid[A])(f: (A) ⇒ B): Valid[B] =
      fa.fold({ error ⇒ error.toString().failureNel }, { res ⇒ f(res).success })
  }

  def open(no: String, name: String, rate: Option[BigDecimal],
           openingDate: Option[Date], accountType: AccountType): AccountOperation[Account]

  def close(no: String, closeDate: Option[Date]): AccountOperation[Account]

  def debit(no: String, amount: Amount): AccountOperation[Account]

  def credit(no: String, amount: Amount): AccountOperation[Account]

  def balance(no: String): AccountOperation[Balance]

  def transfer(from: String, to: String, amount: Amount): AccountOperation[(Account, Account)] =
    for {
      a ← debit(from, amount)
      b ← credit(to, amount)
    } yield (a, b)
}