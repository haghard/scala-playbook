package banking.interpreter

import banking.account.Account
import banking.repository.AccountRepository

import scalaz._
import Scalaz._
import java.util.Date
import banking.service.AccountService
import banking._
import account._
import scalaz.Kleisli._

trait AccountServiceImpl extends AccountService[Account, Amount, Balance] {

  private trait DC

  private case object D extends DC

  private case object C extends DC

  /**
   *
   * @param no
   * @param name
   * @param rate
   * @param openingDate
   * @param accountType
   * @return AccountOperation[Account]
   */
  override def open(no: String, name: String, rate: Option[BigDecimal],
                    openingDate: Option[Date], accountType: AccountType): AccountOperation[Account] =
    kleisli[Valid, AccountRepository, Account] { (repo: AccountRepository) ⇒
      repo.query(no).fold(
        { error ⇒ error.toString().failureNel[Account] }, { account: Option[Account] ⇒
          account.fold(accountType match {
            case Checking ⇒ Account.checkingAccount(no, name, openingDate, None, Balance()).flatMap(repo.store)
            case Savings ⇒ rate map { r ⇒
              Account.savingsAccount(no, name, r, openingDate, None, Balance()).flatMap(repo.store)
            } getOrElse s"Rate needs to be given for savings account".failureNel[Account]
          }) { r ⇒
            s"Already existing account with no $no".failureNel[Account]
          }
        })
    }

  /**
   *
   * @param no
   * @param amount
   * @return
   */
  override def debit(no: String, amount: Amount): AccountOperation[Account] = modify(no, amount, D)

  /**
   *
   * @param no
   * @param amount
   * @return
   */
  override def credit(no: String, amount: Amount): AccountOperation[Account] = modify(no, amount, C)

  /**
   *
   * @param no
   * @param amount
   * @param dc
   * @return AccountOperation[Account]
   */
  private def modify(no: String, amount: Amount, dc: DC): AccountOperation[Account] =
    kleisli[Valid, AccountRepository, Account] { (repo: AccountRepository) ⇒
      repo.query(no).fold(
        { error ⇒ error.toString().failureNel[Account] }, { a: Option[Account] ⇒
          a.fold(s"Account $no does not exist".failureNel[Account]) { a ⇒
            dc match {
              case D ⇒ Account.updateBalance(a, -amount).flatMap(repo.store)
              case C ⇒ Account.updateBalance(a, amount).flatMap(repo.store)
            }
          }
        })
    }

  /**
   *
   * @param no
   * @return AccountOperation[Balance]
   */
  override def balance(no: String): AccountOperation[Balance] =
    kleisli[Valid, AccountRepository, Balance] { (repo: AccountRepository) ⇒ repo.balance(no) }

  /**
   *
   * @param no
   * @param closeDate
   * @return  AccountOperation[Account]
   */
  override def close(no: String, closeDate: Option[Date]): AccountOperation[Account] =
    kleisli[Valid, AccountRepository, Account] { (repo: AccountRepository) ⇒
      repo.query(no).fold(
        { error ⇒ error.toString().failureNel[Account] }, { a: Option[Account] ⇒
          a.fold(s"Account $no does not exist".failureNel[Account]) { a ⇒
            val cd = closeDate.getOrElse(today)
            Account.close(a, cd).flatMap(repo.store)
          }
        })
    }
}

object AccountService extends AccountServiceImpl