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
 * How to abstract Algebraic Data Types with abstract types and a catamorphism:
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
   * Given an Options, we can now speak about one of the types it contains
   * using a type projection, eg. Options#Option[A].
   *
   *
   * Abstract over the type hierarchy in trait
   *
   */
  trait Options {
    type Option[+_]
    type Some[+A] <: Option[A]
    type None <: Option[Nothing]
  }

  /**
   *
   * Abstracting over methods
   * You might be wondering why we need this T as a subtype for Options,
   * as this is usually not needed for typeclasses.
   * It's because we need to be able to project its inner types.
   * Catamorphism over the Options
   */
  abstract class Catamorphism[T <: Options] {
    def some[A](x: A): T#Some[A]

    def none: T#None

    def cata[A, B](opt: T#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B
  }

  /**
   *
   * Let's define a helper to retrieve an instance of Catamorphism[Options] given a signature,
   * if it is available
   */
  object Catamorphism {
    def apply[T <: Options](implicit ops: Catamorphism[T]): Catamorphism[T] = ops
  }

  /**
   *
   * OptionShow[T <: Options : Catamorphism] means that OptionShow is parametrized by a T, which is required to be a subtype of Options.
   * Also an instance of Catamorphism[T] must be implicitly available.
   *
   * New structure that depends on our module.
   *
   */
  import scalaz.Show

  final class OptionShow[T <: Options: Catamorphism] {
    def optionShow[A: Show]: Show[T#Option[A]] = {
      val showA = Show[A]
      val ops = Catamorphism[T]

      new Show[T#Option[A]] {
        override def shows(opt: T#Option[A]): String =
          ops.cata(opt)("none", x ⇒ s"some(${showA.shows(x)})")
      }
    }
  }

  object OptionShow {
    implicit def apply[T <: Options: Catamorphism]: OptionShow[T] = new OptionShow[T]
  }

  trait ScalaOption extends Options {
    type Option[+A] = scala.Option[A]
    type Some[+A] = scala.Some[A]
    type None = scala.None.type
  }

  object ScalaOption {

    implicit object cata extends Catamorphism[ScalaOption] {
      override def some[A](x: A): ScalaOption#Some[A] = scala.Some(x)

      override val none: ScalaOption#None = scala.None

      override def cata[A, B](opt: ScalaOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B =
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

  trait ScalaOption2 extends Options {
    type Option[+A] = scala0.Option[A]
    type Some[+A] = scala0.Some[A]
    type None = scala0.None.type
  }

  object ScalaOption2 extends Options {

    implicit object cata extends Catamorphism[ScalaOption2] {
      override def some[A](x: A): ScalaOption2#Some[A] = scala0.Some(x)

      override val none: ScalaOption2#None = scala0.None

      override def cata[A, B](opt: ScalaOption2#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B =
        opt match {
          case scala0.None    ⇒ ifNone
          case scala0.Some(x) ⇒ ifSome(x)
        }
    }
  }

  trait NullableOption extends Options {
    type Option[+A] = scala.Any
    type Some[+A] = scala.Any
    type None = scala.Null
  }

  object NullableOption {

    implicit object cata extends Catamorphism[NullableOption] {
      override def some[A](x: A): NullableOption#Some[A] = x

      override val none: NullableOption#None = null

      override def cata[A, B](opt: NullableOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B = {
        if (opt == null) ifNone
        else ifSome(opt.asInstanceOf[A])
      }
    }

  }

  trait Java8Option extends Options {
    type Option[+A] = java.util.Optional[_ <: A]
    type Some[+A] = java.util.Optional[_ <: A]
    type None = java.util.Optional[Nothing]
  }

  object Java8Option {

    implicit object cata extends Catamorphism[Java8Option] {
      override def some[A](x: A): Java8Option#Some[A] = java.util.Optional.of(x)

      override val none: Java8Option#None = java.util.Optional.empty()

      override def cata[A, B](opt: Java8Option#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B = {
        def f = new java.util.function.Function[A, B] {
          def apply(a: A): B = ifSome(a)
        }
        def supplier = new java.util.function.Supplier[B] {
          def get(): B = ifNone
        }
        opt.map[B](f).orElseGet(supplier)
      }
    }

  }

  trait GuavaOption extends Options {
    type Option[+A] = com.google.common.base.Optional[_ <: A]
    type Some[+A] = com.google.common.base.Optional[_ <: A]
    type None = com.google.common.base.Optional[Nothing]
  }

  object GuavaOption {
    implicit object cata extends Catamorphism[GuavaOption] {
      override def some[A](x: A): GuavaOption#Some[A] = com.google.common.base.Optional.of(x)

      override val none: GuavaOption#None = com.google.common.base.Optional.absent()

      override def cata[A, B](opt: GuavaOption#Option[A])(ifNone: ⇒ B, ifSome: A ⇒ B): B = {
        def f = new com.google.common.base.Function[A, B] {
          override def apply(a: A): B = ifSome(a)
        }
        def supplier = new com.google.common.base.Supplier[B]() {
          def get(): B = ifNone
        }
        opt.transform(f).or(supplier)
      }
    }
  }

  private[modules] object OptionType extends Enumeration {
    type Type = Value
    val Some, None = Value
  }

  private[modules] case class Program[T <: Options: Catamorphism](implicit tag: ClassTag[T]) {
    private val logger = Logger.getLogger(classOf[Program[T]])
    private val ops = Catamorphism[T]

    def run[A](value: T#Option[A]): OptionType.Type = {
      logger.info(s" ${tag.runtimeClass.getName}")
      ops.cata(value)(
        {
          logger.info("★ ★ ★ This is none ★ ★ ★")
          OptionType.None
        },
        { r ⇒
          logger.info("★ ★ ★ This is some ★ ★ ★")
          OptionType.Some
        })

      //examples
      /*
      import scalaz.std.anyVal.intInstance
      val showOptInt = {
        implicit val showOptInt = OptionShow[T].optionShow[Int]
        OptionShow[T].optionShow[T#Option[Int]]
      }

      val optSome = ops.some(ops.some(13))
      val optNone = ops.some(ops.none)

      import showOptInt.showSyntax.ToShowOps
      logger.info("Options example some: " + ToShowOps(optSome).shows)
      logger.info("Options example none: " + ToShowOps(optNone).shows)
      logger.info("******************************************")
      */
    }
  }
}