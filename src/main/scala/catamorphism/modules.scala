import org.apache.log4j.Logger

import scala.reflect.ClassTag

//http://io.pellucid.com/blog/abstract-algebraic-data-type
/**
 *
 * Scala's sealed class hierarchies have one downside: they don't let us abstract over
 * the type hierarchy as traits and
 * classes are all about constructing new concrete types.
 *
 *
 * If we try than we can find some similarity between 2 processes
 * a) finding concrete implicit value by compiler
 * b) finding concrete dependency by DI framework
 *
 */
package object catamorphism {
  import scala.language.higherKinds

  /**
   *
   * This is just a convenient way to gather several types into a single one, a bit like a record, but for types.
   * Given an OptionSignature, we can now speak about one of the types it contains
   * using a type projection, eg. OptionSignature#Option[A].
   *
   *
   * Abstracting over types
   *
   */
  trait OptionSig {
    type Option[+_]
    type Some[+A] <: Option[A]
    type None <: Option[Nothing]
  }

  /**
   *
   * Abstracting over methods
   * You might be wondering why we need this T as a subtype for OptionSig,
   * as this is usually not needed for typeclasses.
   * It's because we need to be able to project its inner types.
   *
   */
  abstract class OptionOps[T <: OptionSig] {

    def some[A](x: A): T#Some[A]

    def none: T#None

    def fold[A, B](opt: T#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B
  }

  /**
   *
   * Let's define a helper to retrieve an instance of OptionOps[Sig] given a signature,
   * if it is available:
   *
   */
  object OptionOps {
    def apply[T <: OptionSig](implicit ops: OptionOps[T]): OptionOps[T] = ops
  }

  /**
   *
   * OptionShow[T <: OptionSig : OptionOps] means that OptionShow is parameterized by a T,
   * which is required to be a subtype of OptionSig.
   * Also an instance of OptionOps[T] must be implicitly available.
   *
   */
  import scalaz.Show
  final class OptionShow[T <: OptionSig: OptionOps] {

    def optionShow[A: Show]: Show[T#Option[A]] = {
      // retrieving the typeclass instances
      val showA = Show[A]
      val ops = OptionOps[T]

      new Show[T#Option[A]] {
        override def shows(opt: T#Option[A]): String = ops.fold(opt)("none", x ⇒
          s"some(${showA.shows(x)})")
      }
    }
  }

  object OptionShow {
    implicit def apply[T <: OptionSig: OptionOps]: OptionShow[T] = new OptionShow[T]
  }

  trait ScalaOption extends OptionSig {
    type Option[+A] = scala.Option[A]
    type Some[+A] = scala.Some[A]
    type None = scala.None.type
  }

  object ScalaOption {
    implicit object ops extends OptionOps[ScalaOption] {

      def some[A](x: A): ScalaOption#Some[A] = scala.Some(x)

      val none: ScalaOption#None = scala.None

      def fold[A, B](opt: ScalaOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B =
        opt match {
          case scala.None    ⇒ ifNone
          case scala.Some(x) ⇒ ifSome(x)
        }
    }
  }

  object scala0 {
    sealed abstract class Option[+A]
    final case class Some[+A](x: A) extends Option[A]
    case object None extends Option[Nothing]
  }

  trait MyOption extends OptionSig {
    type Option[+A] = scala0.Option[A]
    type Some[+A] = scala0.Some[A]
    type None = scala0.None.type
  }

  object MyOption extends OptionSig {

    implicit object ops extends OptionOps[MyOption] {

      def some[A](x: A): MyOption#Some[A] = scala0.Some(x)

      val none: MyOption#None = scala0.None

      def fold[A, B](opt: MyOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B =
        opt match {
          case scala0.None    ⇒ ifNone
          case scala0.Some(x) ⇒ ifSome(x)
        }
    }
  }

  trait NullOption extends OptionSig {
    type Option[+A] = Any
    type Some[+A] = Any
    type None = Null
  }

  object NullOption {

    implicit object ops extends OptionOps[NullOption] {

      def some[A](x: A): NullOption#Some[A] = x

      val none: NullOption#None = null

      def fold[A, B](opt: NullOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B = {
        if (opt == null) ifNone
        else ifSome(opt.asInstanceOf[A])
      }
    }

  }

  class Program[T <: OptionSig: OptionOps](implicit tag: ClassTag[T]) {
    private val logger = Logger.getLogger(classOf[Program[T]])
    private val ops = OptionOps[T]

    // a little dance to derive our Show instance
    import scalaz.std.anyVal.intInstance
    private val showOptInt = {
      implicit val showOptInt = OptionShow[T].optionShow[Int]
      OptionShow[T].optionShow[T#Option[Int]]
    }

    def run() = {
      logger.info(tag.runtimeClass.getName)

      val optSome = ops.some(ops.some(13))
      val optNone = ops.some(ops.none)

      import showOptInt.showSyntax.ToShowOps

      logger.info("optSome: " + ToShowOps(optSome).shows)
      logger.info("optNone: " + ToShowOps(optNone).shows)
      logger.info("***************************")
    }
  }
}