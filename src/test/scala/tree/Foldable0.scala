package tree

trait Foldable0[F[_]] {
  /**
   *
   *
   */
  def foldMap[T, A](fa: F[T])(f: T ⇒ A)(implicit m: scalaz.Monoid[A]): A
  /**
   *
   *
   */
  def foldMap2[T, A](fa: Option[F[T]])(f: T ⇒ A)(implicit m: scalaz.Monoid[A]): A
  /**
   *
   *
   */
  def foldMapTask[T, A](fa: F[T])(f: T ⇒ A)(implicit m: scalaz.Monoid[A]): scalaz.concurrent.Task[A]
  /**
   *
   *
   */
  def foldRight[T, A](fa: F[T])(z: A)(f: (T, A) ⇒ A): A

  /**
   *
   *
   */
  def foldLeft[T, A](fa: F[T])(z: A)(f: (A, T) ⇒ A): A
}
