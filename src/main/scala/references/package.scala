package object references {

  /**
   * We want our code to not compile if it violates these invariants:
   *
   *  If we hold a reference to a mutable object, then nothing can observe as mutating it.
   *  A mutable object can never be observed outside of the scope in which it was created.
   */

  /**
   *
   * local-effects monad ST,
   * which could stand for state thread,
   * state transition, state token, or state tag.
   *
   */

  sealed trait ST[S, A] { self ⇒

    protected def run(s: S): (A, S)

    def map[B](f: A ⇒ B): ST[S, B] = new ST[S, B] {
      override def run(s: S): (B, S) = {
        val (a, s1) = self.run(s)
        (f(a), s1)
      }
    }

    def flatMap[B](f: A ⇒ ST[S, B]): ST[S, B] = new ST[S, B] {
      override def run(s: S): (B, S) = {
        val (a, s1) = self.run(s)
        f(a).run(s)
      }
    }
  }

  object ST {
    def apply[S, A](a: ⇒ A) = {
      lazy val memo = a //Cache the value in case run is called more than once
      new ST[S, A] {
        override def run(s: S) = (memo, s)
      }
    }

    def run[A](st: TaskST[A]): A =
      st.apply[Unit].run(())._1
  }

  /**
   *
   * @tparam A
   */
  trait TaskST[A] {
    def apply[S]: ST[S, A]
  }

  /**
   * An algebra of mutable references
   *
   */
  final class STReference[S, A] private (private var cell: A = null.asInstanceOf[A]) {

    /**
     *
     *
     */
    def read: ST[S, A] = ST(cell)

    /**
     *
     *
     */
    def write(a: A): ST[S, Unit] = new ST[S, Unit] {
      def run(s: S) = {
        cell = a
        ((), s)
      }
    }
  }

  object STReference {
    def apply[S, A](a: A): ST[S, STReference[S, A]] = ST(new STReference[S, A](a))
  }
}