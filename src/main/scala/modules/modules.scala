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
package object modules {
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
  trait OptionTypes {
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
   * Catamorphism over the OptionTypes
   */
  abstract class Catamorphism[T <: OptionTypes] {

    def some[A](x: A): T#Some[A]

    def none: T#None

    def cata[A, B](opt: T#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B
  }

  /**
   *
   * Let's define a helper to retrieve an instance of OptionOps[Sig] given a signature,
   * if it is available:
   *
   */
  object Catamorphism {
    def apply[T <: OptionTypes](implicit ops: Catamorphism[T]): Catamorphism[T] = ops
  }

  /**
   *
   * OptionShow[T <: OptionTypes : Catamorphism] means that OptionShow is parameterized by a T,
   * which is required to be a subtype of OptionTypes.
   * Also an instance of Catamorphism[T] must be implicitly available.
   *
   */
  import scalaz.Show
  final class OptionShow[T <: OptionTypes: Catamorphism] {

    def optionShow[A: Show]: Show[T#Option[A]] = {
      // retrieving the typeclass instances
      val showA = Show[A]
      val ops = Catamorphism[T]

      new Show[T#Option[A]] {
        override def shows(opt: T#Option[A]): String = ops.cata(opt)("none", x ⇒
          s"some(${showA.shows(x)})")
      }
    }
  }

  object OptionShow {
    implicit def apply[T <: OptionTypes: Catamorphism]: OptionShow[T] = new OptionShow[T]
  }

  trait ScalaOption extends OptionTypes {
    type Option[+A] = scala.Option[A]
    type Some[+A] = scala.Some[A]
    type None = scala.None.type
  }

  object ScalaOption {
    implicit object cata extends Catamorphism[ScalaOption] {

      def some[A](x: A): ScalaOption#Some[A] = scala.Some(x)

      val none: ScalaOption#None = scala.None

      def cata[A, B](opt: ScalaOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B =
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

  trait MyOption extends OptionTypes {
    type Option[+A] = scala0.Option[A]
    type Some[+A] = scala0.Some[A]
    type None = scala0.None.type
  }

  object MyOption extends OptionTypes {

    implicit object ops extends Catamorphism[MyOption] {

      def some[A](x: A): MyOption#Some[A] = scala0.Some(x)

      val none: MyOption#None = scala0.None

      def cata[A, B](opt: MyOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B =
        opt match {
          case scala0.None    ⇒ ifNone
          case scala0.Some(x) ⇒ ifSome(x)
        }
    }
  }

  trait NullOption extends OptionTypes {
    type Option[+A] = Any
    type Some[+A] = Any
    type None = Null
  }

  object NullOption {

    implicit object ops extends Catamorphism[NullOption] {

      def some[A](x: A): NullOption#Some[A] = x

      val none: NullOption#None = null

      def cata[A, B](opt: NullOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B = {
        if (opt == null) ifNone
        else ifSome(opt.asInstanceOf[A])
      }
    }
  }

  trait Java8Option extends OptionTypes {
    type Option[+A] = java.util.Optional[_ <: A]
    type Some[+A] = java.util.Optional[_ <: A]
    type None = java.util.Optional[Nothing]

  }

  object Java8Option {
    implicit object ops extends Catamorphism[Java8Option] {

      def some[A](x: A): Java8Option#Some[A] = java.util.Optional.of(x)

      val none: Java8Option#None = java.util.Optional.empty()

      def cata[A, B](opt: Java8Option#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B = {
        import java.util.function.{ Function ⇒ F, Supplier }
        def f = new F[A, B] { def apply(a: A): B = ifSome(a) }
        def supplier = new Supplier[B] { def get(): B = ifNone }
        opt.map[B](f).orElseGet(supplier)
      }
    }
  }

  class Program[T <: OptionTypes: Catamorphism](implicit tag: ClassTag[T]) {
    private val logger = Logger.getLogger(classOf[Program[T]])
    private val ops = Catamorphism[T]

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
      logger.info("*************************************")
    }
  }
}