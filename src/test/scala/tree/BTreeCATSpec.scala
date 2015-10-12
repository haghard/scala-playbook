import org.specs2.mutable.Specification
import scala.language.higherKinds

//Ideas are borrowed from https://github.com/jsuereth/cat.git
class BTreeCATSpec extends Specification {

  sealed trait BTree[T]
  case class Leaf[T](elem: T) extends BTree[T]
  case class Node[T](left: BTree[T], right: BTree[T]) extends BTree[T]

  object BTree {
    def leaf(): BTree[Unit] = new Leaf[Unit](())
    def leaf[T](t: T): BTree[T] = new Leaf[T](t)
    def bin[T](l: BTree[T], r: BTree[T]): BTree[T] = new Node(l, r)

    def print[T](t: BTree[T]): Unit = {
      def printLoop(pad: String, t: BTree[T]): Unit = {
        t match {
          case Leaf(elem) ⇒ System.out.println(s"$pad$elem")
          case Node(l, r) ⇒
            System.out.println(s"$pad+-\\")
            printLoop(pad + "| ", l)
            printLoop(pad + "  ", r)
        }
      }
      printLoop("", t)
    }
  }

  trait Pure[F[_]] {
    def pure[A](a: A): F[A]
  }

  trait Traversable[A[_]] {
    def traverse[F[_]: ApplicativeFunctor, X, Y](col: A[X])(f: X ⇒ F[Y]): F[A[Y]]
  }

  trait ApplicativeFunctor[F[_]] extends scalaz.Functor[F] with Pure[F] {
    def ap[A, B](fa: F[A])(fb: F[A ⇒ B]): F[B]
    def map2[A, B, C](fa: F[A], fb: F[B])(f: (A, B) ⇒ C): F[C] = {
      val nextFunc = map(fb) { b ⇒ (a: A) ⇒ f(a, b) }
      ap(fa)(nextFunc)
    }
  }

  trait State[S, A] {
    def run(initial: S): (S, A)
    def map[B](f: A ⇒ B): State[S, B] = State { s ⇒
      val (rs, ra) = run(s)
      (rs, f(ra))
    }
    def flatMap[B](f: A ⇒ State[S, B]): State[S, B] =
      State { s ⇒
        val (s1, x1) = run(s)
        f(x1).run(s1)
      }
  }

  object State {
    def apply[S, A](f: S ⇒ (S, A)): State[S, A] = {
      object FullState extends State[S, A] {
        def run(initial: S): (S, A) = f(initial)
      }
      FullState
    }

    def apply[S, A](el: A): State[S, A] = {
      object Const extends State[S, A] {
        def run(initial: S): (S, A) = (initial, el)
      }
      Const
    }
  }

  object implicits {
    implicit object optionInstances extends ApplicativeFunctor[Option] {
      override def ap[X, Y](fa: Option[X])(fb: Option[(X) ⇒ Y]): Option[Y] = for { a ← fa; f ← fb } yield f(a)
      override def map[X, Y](fa: Option[X])(x: (X) ⇒ Y): Option[Y] = fa map x
      override def pure[X](a: X): Option[X] = Option(a)
    }

    implicit def compose[F[_]: ApplicativeFunctor, M[_]: ApplicativeFunctor] =
      new ApplicativeFunctor[({ type λ[x] = F[M[x]] })#λ] {
        private val tt = implicitly[ApplicativeFunctor[F]]
        private val tu = implicitly[ApplicativeFunctor[M]]
        override def ap[X, Y](fa: F[M[X]])(fb: F[M[(X) ⇒ Y]]): F[M[Y]] = {
          tt.map2(fa, fb) { (ux: M[X], ub: M[(X ⇒ Y)]) ⇒
            tu.map2(ux, ub) { (x, f) ⇒ f(x) }
          }
        }
        override def pure[X](a: X): F[M[X]] = tt.pure(tu.pure(a))
        override def map[X, Y](fa: F[M[X]])(x: (X) ⇒ Y): F[M[Y]] = tt.map(fa) { ux: M[X] ⇒ tu.map(ux)(x) }
      }

    implicit object treeTraversable extends Traversable[BTree] {
      override def traverse[F[_]: ApplicativeFunctor, X, Y](col: BTree[X])(f: (X) ⇒ F[Y]): F[BTree[Y]] = {
        val apF = implicitly[ApplicativeFunctor[F]]
        col match {
          case Leaf(a) ⇒ apF.ap(f(a))(apF.pure(BTree.leaf(_)))
          case Node(l, r) ⇒
            val fl = traverse(l)(f)
            val fr = traverse(r)(f)
            apF.map2(fl, fr)(BTree.bin(_, _))
        }
      }
    }

    implicit def stateApplicativeFunctor[S]: ApplicativeFunctor[({ type λ[x] = State[S, x] })#λ] =
      new ApplicativeFunctor[({ type λ[x] = State[S, x] })#λ] {
        override def ap[X, Y](fa: State[S, X])(fb: State[S, X ⇒ Y]): State[S, Y] = for { x ← fa; f ← fb } yield f(x)
        override def map[X, Y](fa: State[S, X])(f: X ⇒ Y): State[S, Y] = fa.map(f)
        override def pure[X](a: X): State[S, X] = State.apply(a)
      }
  }

  import implicits._

  def fill[T[_]: Traversable, E](ctx: T[Unit], values: Seq[E]): Option[T[E]] = {
    type FillState[X] = State[Seq[E], X]
    def takeHead: FillState[Option[E]] = State { s ⇒
      s match {
        case Nil             ⇒ (Nil, None)
        case Seq(x, xs @ _*) ⇒ (xs, Some(x))
      }
    }
    val tt = implicitly[Traversable[T]]
    type Context[X] = FillState[Option[X]]
    val result = tt.traverse[Context, Unit, E](ctx)(_ ⇒ takeHead)
    result.run(values)._2
  }

  "Construct BTree from source" should {
    "prettyPrint" in {
      import BTree._

      val emptyTree: BTree[Unit] = {
        bin(
          bin(
            bin(leaf(), leaf()),
            leaf()
          ),
          bin(
            bin(
              leaf(),
              bin(leaf(), leaf())
            ),
            bin(leaf(),
              bin(leaf(), leaf()))
          )
        )
      }

      val tree = fill(emptyTree, Seq.range(1, 50))
      BTree.print(tree.get)
      1 === 1
    }
  }
}