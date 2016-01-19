package tree

import mongo.MongoProgram.NamedThreadFactory
import org.specs2.mutable.Specification

import scalaz._, Scalaz._
import scalaz.concurrent.Task

class ScalazTree extends Specification {

  implicit val Executor = java.util.concurrent.Executors.newFixedThreadPool(4, new NamedThreadFactory("pTree"))

  def monoidPar[T: Monoid, M[_]: Monad : Nondeterminism]: Monoid[M[T]] = new Monoid[M[T]] {
    val m = implicitly[Monoid[T]]
    val M = implicitly[Monad[M]]
    val ND = implicitly[Nondeterminism[M]]

    override val zero = M.pure(m.zero)

    override def append(a: M[T], b: ⇒ M[T]): M[T] =
      ND.nmap2(a, b) { (l, r) ⇒
        val res = m.append(l, r)
        println(s"${Thread.currentThread().getName}: $l and $r = $res")
        res
      }
  }

  "Tree" should {
    "fold with parallel monoid" in {

      val PM = monoidPar[Int, Task]

      val tree = Task { 10 }.node(
        Task { Thread.sleep(500); 2 }.node(
          Task { Thread.sleep(100); 3 }.node(Task { Thread.sleep(200); 4 }.leaf, Task { Thread.sleep(100); 5 }.leaf),
          Task { Thread.sleep(100); 4 }.leaf, Task { 5 }.leaf),
        Task { Thread.sleep(100); 6 }.node(Task { 7 }.leaf, Task { Thread.sleep(100); 8 }.leaf, Task { 9 }.leaf)
      )

      println(tree.map(_.run).drawTree)
      val r = tree.foldMap(identity)(PM)
      r.run === 63
    }
  }
}