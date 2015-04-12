package tree

import java.util.concurrent.Executors._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import mongo.MongoProgram.NamedThreadFactory
import org.apache.log4j.Logger
import org.specs2.mutable.Specification
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{ Await, Promise, ExecutionContext, Future }
import scalaz._
import scala.concurrent.duration._
import Scalaz._
import scala.language.higherKinds
import scalaz.concurrent.Task

class TreeSpec extends Specification {
  implicit val M = scalaz.Monoid[Int]

  "BTree" should {
    "foreach and size" in {
      var jobs = List("0", "1a", "2a", "2b",
        "3a", "3b", "3c", "3d", "1b", "2c", "2d", "3i", "3f", "3g", "3h")

      def next = {
        val n = jobs.head
        jobs = jobs.tail
        n
      }

      //tree foreach (logger.info(_))
      val tree = BTree(4)(next)
      tree.size should be equalTo 15
    }
  }

  "BTree" should {
    "map" in {
      val f = (x: Int) ⇒ x * 2

      val i = new AtomicInteger()
      val tree = BTree(3)(i.incrementAndGet) map f

      tree.size should be equalTo 7
      BTree.foldMap(tree)(identity) should be equalTo 28 * 2
    }
  }

  "BTree" should {
    "folds" in {
      val f = (x: Int) ⇒ x * 3

      val i = new AtomicInteger()
      val t = BTree(3)(i.incrementAndGet())

      //28 is a sum of default 3 level deep tree
      BTree.foldMap(t)(f) should be equalTo f(28)
      BTree.foldMap2(Option(t))(f) should be equalTo f(28)
      BTree.foldRight(t)(0)(_ + _) should be equalTo 28
      BTree.foldLeft(t)(0)(_ + _) should be equalTo 28
    }
  }

  "BTree" should {
    "foldMap foldMap2 vs foldMapPar2" in {
      val m: (Int) ⇒ Int =
        x ⇒ {
          //simulate job
          Thread.sleep(10)
          x
        }

      val i = new AtomicInteger()
      val t = BTree(10)(i.incrementAndGet())
      //BTree.foldMap(t)(m) should be equalTo 523776 // 12 sec
      BTree.foldMap2(Option(t))(m) should be equalTo 523776 // 12 sec
      BTree.foldMapPar(t)(m).run should be equalTo 523776 //2 sec
    }
  }

  "BTree" should {
    "mapAsync" in {
      implicit val ec = ExecutionContext.fromExecutor(
        newFixedThreadPool(3, new NamedThreadFactory("tree-walker")))

      val f = (x: Int) ⇒ x * 3

      val i = new AtomicInteger()
      val tree = BTree(3)(i.incrementAndGet())

      val rf = BTree sequence (tree mapAsync f)
      val newTree = Await.result(rf, new FiniteDuration(5, TimeUnit.SECONDS))
      //28 is a sum of default 3 level deep tree
      BTree.foldMap(newTree)(identity) should be equalTo f(28)
    }
  }
}

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
  def foldMapPar[T, A](fa: BTree[T])(f: T ⇒ A)(implicit m: Monoid[A]): Task[A]
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

object BTree extends Foldable0[BTree] {
  private val logger = Logger.getLogger("btree")

  def apply[T](levelNumber: Int)(block: ⇒ T): BTree[T] =
    levelNumber match {
      case 1 ⇒ new BTree(block, None, None)
      case _ ⇒ new BTree(block, Some(BTree(levelNumber - 1)(block)), Some(BTree(levelNumber - 1)(block)))
    }

  def sequence[T](tree: BTree[Future[T]])(implicit ec: ExecutionContext): Future[BTree[T]] = {
    import scala.concurrent.duration._
    val pending = new AtomicInteger(tree.size)
    val p = Promise[BTree[T]]()

    //foreach
    for (future ← tree; v ← future) {
      if (pending.decrementAndGet == 0)
        p.success(tree map { f: Future[T] ⇒ Await.result(f, 0 seconds) })
    }
    p.future
  }

  private[BTree] def apply[T](value: T, left: Option[BTree[T]], right: Option[BTree[T]]) =
    new BTree[T](value, left, right)

  @annotation.tailrec
  private[BTree] def loop[T, A](tree: Option[BTree[T]], rest: List[Option[BTree[T]]], f: T ⇒ A, m: (A, ⇒ A) ⇒ A, acc: A): A = {
    tree match {
      case Some(t)            ⇒ loop(t.left, t.right :: rest, f, m, m(acc, f(t.v)))
      case _ if (rest != Nil) ⇒ loop(rest.head, rest.tail, f, m, acc)
      case _                  ⇒ acc
    }
  }

  @annotation.tailrec
  private[BTree] def go[T, U](tree: Option[BTree[T]], rest: List[Option[BTree[T]]], f: T ⇒ U): Unit = {
    tree match {
      case Some(t) ⇒
        f(t.v) //evaluation
        go(t.left, t.right :: rest, f)
      case _ ⇒
        if (rest != Nil) go(rest.head, rest.tail, f)
    }
  }

  override def foldLeft[T, A](fa: BTree[T])(z: A)(f: (A, T) ⇒ A): A = {
    import Dual._, Endo._, syntax.std.all._
    Tag.unwrap(foldMap(fa)((a: T) ⇒ Dual(Endo.endo(f.flip.curried(a))))(dualMonoid)) apply (z)
  }

  override def foldRight[T, A](fa: BTree[T])(z: A)(f: (T, A) ⇒ A): A =
    foldMap(fa)((a: T) ⇒ Endo.endo(f(a, _: A))) apply z

  override def foldMap[T, A](tree: BTree[T])(f: T ⇒ A)(implicit m: Monoid[A]): A =
    loop(tree.right, List(), f, m.append,
      loop(tree.left, List(), f, m.append, m.append(m.zero, f(tree.v))))

  override def foldMap2[T, A](tree: Option[BTree[T]])(f: T ⇒ A)(implicit m: Monoid[A]): A = tree match {
    case Some(c) ⇒
      c.left.fold(m.append(m.zero, f(c.v))) { _ ⇒
        m.append(f(c.v), m.append(foldMap2(c.left)(f), foldMap2(c.right)(f)))
      }
  }

  private[BTree] val st = newFixedThreadPool(8, new NamedThreadFactory("tree-folder"))

  private[BTree] implicit def monoidT[A](m: Monoid[A]): Monoid[Task[A]] = new Monoid[Task[A]] {
    private val ND = Nondeterminism[Task]

    override def zero = Task.delay(m.zero)

    override def append(a: Task[A], b: ⇒ Task[A]): Task[A] =
      for {
        r ← ND.nmap2(Task.fork(a)(st), Task.fork(b)(st)) { (l, r) ⇒
          //logger.info(s" op($l,$r)")
          m.append(l, r)
        }
      } yield r
  }

  override def foldMapPar[T, B](t: BTree[T])(f: T ⇒ B)(implicit m: Monoid[B]): Task[B] =
    Task delay (Option(t)) flatMap { tree ⇒
      foldMap2(tree)(element ⇒ Task.delay {
        //Thread.sleep(ThreadLocalRandom.current().nextInt(50, 100));
        //logger.info(s"read $element")
        f(element)
      })(monoidT(m))
    }
}

class BTree[T] private[BTree] (val v: T, val left: Option[BTree[T]], val right: Option[BTree[T]]) extends Traversable[T] {
  import BTree._
  private val logger = Logger.getLogger("btree")

  override def foreach[U](f: T ⇒ U): Unit = {
    f(v)
    go(left, List(), f)
    go(right, List(), f)
  }

  override def size(): Int = {
    @annotation.tailrec
    def go(acc: Int, tree: Option[BTree[T]], rest: List[Option[BTree[T]]]): Int = {
      tree match {
        case Some(tr) ⇒ go(1 + acc, tr.left, tr.right :: rest)
        case _        ⇒ if (rest == Nil) acc else go(acc, rest.head, rest.tail)
      }
    }
    1 + go(0, left, List()) + go(0, right, List())
  }

  /**
   * Create new tree with updated values
   * @param f
   * @tparam B
   * @return
   */
  def map[B](f: T ⇒ B): BTree[B] =
    BTree(f(v),
      left.map(_.map(f)),
      right.map(_.map(f)))

  /**
   * Create new tree with updated values
   * @param f
   * @param ex
   * @tparam B
   * @return
   */
  def mapAsync[B](f: T ⇒ B)(implicit ex: ExecutionContext): BTree[Future[B]] =
    BTree(
      Future {
        val r = f(v)
        logger.info(s"start eval $r")
        Thread.sleep(ThreadLocalRandom.current().nextLong(100, 400)) //for testing ...
        logger.info(s"end eval $r")
        r
      },
      left.map(_.mapAsync(f)),
      right.map(_.mapAsync(f)))
}