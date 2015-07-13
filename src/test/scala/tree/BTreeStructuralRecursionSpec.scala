package tree

import java.util.concurrent.Executors

import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger

import scala.annotation.tailrec
import org.specs2.mutable.Specification

import scala.concurrent.forkjoin.ThreadLocalRandom
import scalaz.Monoid
import scalaz.concurrent.Task
import scalaz._
import Scalaz._

class BTreeStructuralRecursionSpec extends Specification {
  import tree.{ node, invert, foldLeft, foldMap, foldMapPar, depth }

  implicit val M = scalaz.Monoid[Int]

  private val logger = Logger.getLogger("btree")

  "Tree" should {
    "has been foldLeft/invert/foldLeft properly" in {
      val tree = node(4,
        node(2,
          node(3)
        ),
        node(7, node(6), node(9)
        )
      )

      foldLeft(tree)(0)(_ + _) === 33
      println("Invert")
      val invertedTree = invert(tree)
      foldLeft(invertedTree)(0)(_ + _) === 31
    }
  }

  "Btree" should {
    "has been constructed properly" in {
      var t = node(5)
      t = t :+ 4
      t = t :+ 10
      t = t :+ 11
      t = t :+ 3
      t = t :+ 9

      foldLeft(t)(0)(_ + _) === 42
    }
  }

  "Btree" should {
    "has been constructed and searched" in {
      var t = node(5)
      t = t :+ 4
      t = t :+ 10
      t = t :+ 11
      t = t :+ 3
      t = t :+ 9
      t = t :+ 8
      t = t :+ 2
      t = t :+ 7

      t.search(7) === Some(7)
    }
  }

  "Btree" should {
    "has been foldMap" in {
      var t = node(5)
      t = t :+ 4
      t = t :+ 10
      t = t :+ 11
      t = t :+ 3
      t = t :+ 9

      foldMap(t)(identity) === 42
    }
  }

  "Btree" should {
    "has been foldMap" in {
      val uniqueItems = List(1, 90, 20, 60, 40, 50, 30, 70, 80, 10, 15, 26, 37, 41, 22, 33, 44, 55)
      val tree = uniqueItems.foldLeft(node(49))(_ :+ _)

      val m: (Int) ⇒ Int =
        x ⇒ {
          Thread.sleep(ThreadLocalRandom.current().nextInt(100, 200))
          x
        }

      val actual = foldMapPar(tree)(m).run
      val expected = uniqueItems.fold(49)(_ + _)
      actual === expected
    }
  }

  "Btree" should {
    "depth" in {

      var t = node(5)
      t = t :+ 4
      t = t :+ 10
      t = t :+ 11
      t = t :+ 3
      t = t :+ 9
      t = t :+ 15

      depth(t) === 4
    }
  }

  "Btree" should {
    "maximum" in {
      var t = node(5)
      t = t :+ 4
      t = t :+ 10
      t = t :+ 11
      t = t :+ 3
      t = t :+ 9
      t = t :+ 15

      val m = t.maximum
      m === 15
    }
  }

  sealed trait Tree[+T]

  case object Leaf extends Tree[Nothing]

  case class Node[T](value: T, left: Tree[T], right: Tree[T]) extends Tree[T]

  object tree extends Foldable0[Tree] {

    def nil[A]: Tree[A] = Leaf

    def node[A](value: A, left: Tree[A] = nil, right: Tree[A] = nil): Tree[A] =
      Node(value, left, right)

    private def foldLoop[A, B](t: Tree[A], z: B)(f: (B, A, B) ⇒ B): B = t match {
      case Leaf          ⇒ z
      case Node(v, l, r) ⇒ f(foldLoop(l, z)(f), v, foldLoop(r, z)(f))
    }

    def map[A, B](tree: Tree[A])(f: A ⇒ B): Tree[B] =
      foldLoop(tree, nil[B]) { (left, value, right) ⇒ Node(f(value), left, right) }

    def invert[A](tree: Tree[A]): Tree[A] =
      foldLoop(tree, nil[A]) { (left, value, right) ⇒ Node(value, right, left) }

    def size[T](tree: Tree[T]): Int = foldLoop(tree, 0: Int) { (l, x, r) ⇒ l + r + 1 }

    def depth[A](tree: Tree[A]): Int = foldLoop(tree, 1: Int) { (l, v, r) ⇒ 1 + l max r }

    override def foldLeft[T, A](as: Tree[T])(z: A)(f: (A, T) ⇒ A): A = as match {
      case Leaf ⇒ z
      case Node(v, Leaf, Leaf) ⇒
        val acc = f(z, v)
        println(s"$v - $acc")
        acc
      case Node(v, l, r) ⇒
        val acc = f(z, v)
        println(s"$v - $acc")
        foldLeft(r)(foldLeft(l)(acc)(f))(f)
    }

    override def foldRight[T, A](as: Tree[T])(z: A)(f: (T, A) ⇒ A): A = as match {
      case Leaf ⇒ z
      case Node(v, Leaf, Leaf) ⇒
        val acc = f(v, z)
        println(s"$v - $acc")
        acc
      case Node(v, l, r) ⇒
        val acc = f(v, z)
        println(s"$v - $acc")
        foldRight(l)(foldRight(r)(acc)(f))(f)
    }

    override def foldMap[T, A](fa: Tree[T])(f: (T) ⇒ A)(implicit m: Monoid[A]): A = {
      def foldMap0(fa: Tree[T], n: Long = 0l)(f: (T) ⇒ A)(implicit m: Monoid[A]): A =
        fa match {
          case Leaf                ⇒ m.zero
          case Node(v, Leaf, Leaf) ⇒ f(v)
          case Node(v, l, r) ⇒
            if (n % 2 == 0) m.append(foldMap0(l, n + 1)(f)(m), m.append(f(v), foldMap0(r, n + 1)(f)(m)))
            else m.append(m.append(f(v), foldMap0(l, n + 1)(f)(m)), foldMap0(r, n + 1)(f)(m))
        }
      foldMap0(fa)(f)
    }

    override def foldMap2[T, A](fa: Option[Tree[T]])(f: (T) ⇒ A)(implicit m: Monoid[A]): A = ???

    override def foldMapPar[T, A](fa: Tree[T])(f: (T) ⇒ A)(implicit M: Monoid[A]): Task[A] =
      Task.now(fa).flatMap { foldMap(_)(e ⇒ Task.delay(f(e)))(monoidPar(M)) }

    def monoidPar[A](m: Monoid[A]): Monoid[Task[A]] = new Monoid[Task[A]] {
      implicit val S = Executors.newFixedThreadPool(3, new NamedThreadFactory("btree-par-monoid"))
      private val ND = Nondeterminism[Task]

      override def zero = Task.delay(m.zero)

      override def append(a: Task[A], b: ⇒ Task[A]): Task[A] =
        for {
          r ← ND.nmap2(Task.fork(a)(S), Task.fork(b)(S)) { (l, r) ⇒
            logger.info(s" op($l,$r)")
            m.append(l, r)
          }
        } yield r
    }
  }

  implicit class TreeSyntax[T](self: Tree[T])(implicit ord: scala.math.Ordering[T]) {

    private def maximum0(v: T, t: Tree[T]): T = t match {
      case Leaf ⇒ v
      case Node(v, left, right) ⇒
        val left = maximum0(v, left)
        val right = maximum0(v, right)
        if (ord.lt(left, right)) right else left
    }

    def maximum: T = maximum0(null.asInstanceOf[T], self)

    @tailrec private def scan(searched: T, t: Tree[T]): Option[T] = t match {
      case Leaf                                  ⇒ None
      case Node(v, left, right) if searched == v ⇒ Option(v)
      case Node(v, left, right) ⇒
        if (ord.lt(searched, v)) {
          println(s"Search: passed: $v")
          scan(searched, left)
        } else {
          println(s"Search: passed: $v")
          scan(searched, right)
        }
    }

    def search(searched: T): Option[T] =
      scan(searched, self)

    def :+(v: T): Tree[T] = {
      (v, self) match {
        case (value, Leaf) ⇒ node(value)
        case (inserted, Node(a, left, right)) if inserted == a ⇒
          Node(inserted, left, right)
        case (inserted, Node(a, left, right)) if ord.lt(inserted, a) ⇒
          Node(a, left :+ inserted, right)
        case (inserted, Node(a, left, right)) if ord.gt(inserted, a) ⇒
          Node(a, left, right :+ inserted)
      }
    }

  }
}