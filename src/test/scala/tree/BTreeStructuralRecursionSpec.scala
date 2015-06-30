package tree

import org.specs2.mutable.Specification

class BTreeStructuralRecursionSpec extends Specification {
  import tree._

  "Tree" should {
    "has been foldLeft/invert/foldLeft properly" in {
      val tree = node(4,
        node(2,
          node(3)
        ),
        node(7, node(6), node(9)
        )
      )

      foldLeft(tree)(0)(_ + _) === 31
      println("*******Invert**********")
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

  sealed trait Tree[+T]
  case object Leaf extends Tree[Nothing]
  case class Node[T](value: T, left: Tree[T], right: Tree[T]) extends Tree[T]

  object tree {
    def nil[A]: Tree[A] = Leaf
    def node[A](value: A, left: Tree[A] = nil, right: Tree[A] = nil): Tree[A] =
      Node(value, left, right)

    private def foldLoop[A, B](t: Tree[A], z: B)(f: (B, A, B) ⇒ B): B = t match {
      case Leaf          ⇒ z
      case Node(v, l, r) ⇒ f(foldLoop(l, z)(f), v, foldLoop(r, z)(f))
    }

    def size[T](tree: Tree[T]) = foldLoop(tree, 0: Int) { (l, x, r) ⇒ l + r + 1 }

    def foldLeft[A, B](as: Tree[A])(z: B)(f: (B, A) ⇒ B): B = as match {
      case Node(v, Leaf, Leaf) ⇒
        val acc = f(z, v)
        println(s"$v - $acc")
        acc
      case Node(v, l, r) ⇒
        val acc = f(z, v)
        println(s"$v - $acc")
        foldLeft(r)(foldLeft(l)(acc)(f))(f)
      case Leaf ⇒ z
    }

    def foldRight[A, B](as: Tree[A])(z: B)(f: (B, A) ⇒ B): B = as match {
      case Node(v, Leaf, Leaf) ⇒
        val acc = f(z, v)
        println(s"$v - $acc")
        acc
      case Node(v, l, r) ⇒
        val acc = f(z, v)
        println(s"$v - $acc")
        foldRight(l)(foldRight(r)(acc)(f))(f)
    }

    def map[A, B](tree: Tree[A])(f: A ⇒ B): Tree[B] =
      foldLoop(tree, nil[B]) { (left, value, right) ⇒ Node(f(value), left, right) }

    def invert[A](tree: Tree[A]): Tree[A] =
      foldLoop(tree, nil[A]) { (left, value, right) ⇒ Node(value, right, left) }

    implicit class TreeSyntax[T](self: Tree[T]) {

      def :+(v: T)(implicit ord: Ordering[T]): Tree[T] = {
        (v, self) match {
          case (value, Leaf) ⇒ node(value)
          case (inserted, Node(a, left, right)) if inserted == a ⇒
            Node(inserted, left, right)
          case (inserted, Node(a, left, right)) if ord.lt(inserted, a) ⇒
            Node(a, left :+ (inserted), right)
          case (inserted, Node(a, left, right)) if ord.gt(inserted, a) ⇒
            Node(a, left, right :+ (inserted))
        }
      }
    }
  }
}
