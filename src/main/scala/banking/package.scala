
/**
 *
 * Slightly modified example From: Debasish Ghosh.Functional and Reactive Domain Modeling MEAP
 */
package object banking {

  import scalaz._
  import Scalaz._
  import Kleisli._
  import java.util.Calendar
  import java.util.Date

  type Amount = BigDecimal

  sealed trait AccountType
  case object Checking extends AccountType
  case object Savings extends AccountType

  case class Balance(amount: Amount = 0)

  object account {

    def today = Calendar.getInstance.getTime

    case class Address(no: String, street: String, city: String, state: String, zip: String)

    final case class CheckingAccount(no: String, name: String,
                                     dateOfOpen: Option[Date], dateOfClose: Option[Date] = None,
                                     balance: Balance = Balance()) extends Account

    final case class SavingsAccount(no: String, name: String, rateOfInterest: Amount,
                                    dateOfOpen: Option[Date], dateOfClose: Option[Date] = None,
                                    balance: Balance = Balance()) extends Account
    sealed trait Account {
      def no: String

      def name: String

      def dateOfOpen: Option[Date]

      def dateOfClose: Option[Date]

      def balance: Balance
    }

    object Account {
      private def validateAccountNo(no: String): ValidationNel[String, String] =
        if (no.isEmpty || no.size < 5) s"Account No has to be at least 5 characters long: found $no".failureNel[String]
        else no.successNel[String]

      private def validateOpenCloseDate(od: Date, cd: Option[Date]): ValidationNel[String, (Option[Date], Option[Date])] = cd.map { c ⇒
        if (c before od) s"Close date [$c] cannot be earlier than open date [$od]".failureNel[(Option[Date], Option[Date])]
        else (od.some, cd).successNel[String]
      }.getOrElse((od.some, cd).successNel[String])

      private def validateRate(rate: BigDecimal) =
        if (rate <= BigDecimal(0)) s"Interest rate $rate must be > 0".failureNel[BigDecimal]
        else rate.successNel[String]

      def checkingAccount(no: String, name: String, openDate: Option[Date], closeDate: Option[Date],
                          balance: Balance): ValidationNel[String, Account] = {
        (validateAccountNo(no) |@| validateOpenCloseDate(openDate.getOrElse(today), closeDate)) {
          case (n, d) ⇒ CheckingAccount(n, name, d._1, d._2, balance)
        }
      }

      def savingsAccount(no: String, name: String, rate: BigDecimal, openDate: Option[Date],
                         closeDate: Option[Date], balance: Balance): ValidationNel[String, Account] = {
        (validateAccountNo(no) |@| validateOpenCloseDate(openDate.getOrElse(today), closeDate) |@| validateRate(rate)) { (n, d, r) ⇒
          SavingsAccount(n, name, r, d._1, d._2, balance)
        }
      }

      private def validateAccountAlreadyClosed(a: Account) = {
        if (a.dateOfClose isDefined) s"Account ${a.no} is already closed".failureNel[Account]
        else a.success
      }

      private def validateCloseDate(a: Account, cd: Date) = {
        if (cd before a.dateOfOpen.get) s"Close date [$cd] cannot be earlier than open date [${a.dateOfOpen.get}]".failureNel[Date]
        else cd.success
      }

      def close(a: Account, closeDate: Date): ValidationNel[String, Account] = {
        (validateAccountAlreadyClosed(a) |@| validateCloseDate(a, closeDate)) { (acc, d) ⇒
          acc match {
            case c: CheckingAccount ⇒ c.copy(dateOfClose = Some(closeDate))
            case s: SavingsAccount  ⇒ s.copy(dateOfClose = Some(closeDate))
          }
        }
      }

      private def checkBalance(a: Account, amount: Amount) = {
        if (amount < 0 && a.balance.amount < -amount) s"Insufficient amount in ${a.no} to debit".failureNel[Account]
        else a.success
      }

      def updateBalance(a: Account, amount: Amount): ValidationNel[String, Account] = {
        (validateAccountAlreadyClosed(a) |@| checkBalance(a, amount)) { (_, _) ⇒
          a match {
            case c: CheckingAccount ⇒ c.copy(balance = Balance(c.balance.amount + amount))
            case s: SavingsAccount  ⇒ s.copy(balance = Balance(s.balance.amount + amount))
          }
        }
      }

      def rate(a: Account) = a match {
        case SavingsAccount(_, _, r, _, _, _) ⇒ r.some
        case _                                ⇒ None
      }
    }

  }

  import account._

  trait AccountRepository {
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
        { error ⇒ s"No account exists with no $no".failureNel[Balance] },
        { a: Option[Account] ⇒
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

  type Valid[A] = ValidationNel[String, A]

  trait AccountService[Account, Amount, Balance] {
    type AccountOperation[A] = Kleisli[Valid, AccountRepository, A]

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
      } yield ((a, b))
  }

  trait AccountServiceInterpreter extends AccountService[Account, Amount, Balance] {

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
          { error ⇒ error.toString().failureNel[Account] },
          { account: Option[Account] ⇒
            account.fold(accountType match {
              case Checking ⇒ Account.checkingAccount(no, name, openingDate, None, Balance()).flatMap(repo.store)
              case Savings ⇒ rate map { r ⇒
                Account.savingsAccount(no, name, r, openingDate, None, Balance()).flatMap(repo.store)
              } getOrElse (s"Rate needs to be given for savings account".failureNel[Account])
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
          { error ⇒ error.toString().failureNel[Account] },
          { a: Option[Account] ⇒
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
          { error: NonEmptyList[String] ⇒ error.toString().failureNel[Account] },
          { a: Option[Account] ⇒
            a.fold(s"Account $no does not exist".failureNel[Account]) { a ⇒
              val cd = closeDate.getOrElse(today)
              Account.close(a, cd).flatMap(repo.store)
            }
          })
      }
  }

  /**
   *
   * @tparam Amount
   */
  trait ReportingService[Amount] {
    type ReportOperation[A] = Kleisli[Valid, AccountRepository, A]

    /**
     *
     * @return
     */
    def balances: ReportOperation[Seq[(String, Amount)]]
  }

  trait ReportingServiceInterpreter extends ReportingService[Amount] {
    override def balances: ReportOperation[Seq[(String, Amount)]] =
      kleisli[Valid, AccountRepository, Seq[(String, Amount)]] { (repo: AccountRepository) ⇒
        repo.all.fold(
          { error ⇒ error.toString().failureNel
          }, { as: Seq[Account] ⇒
            as.map(a ⇒ (a.no, a.balance.amount)).success
          })
      }
  }

  object AccountService extends AccountServiceInterpreter
  object ReportingService extends ReportingServiceInterpreter
}