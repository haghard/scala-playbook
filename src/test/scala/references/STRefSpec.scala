package references

import org.specs2.mutable.Specification

class STRefSpec extends Specification {

  "STRef" should {
    "run local effects mutation" in {

      val p0 = new TaskST[(Float, Float)] {
        override def apply[S] =
          STReference(5.78f).flatMap { x ⇒
            STReference(5.77f).flatMap { y ⇒
              x.read.flatMap { x0 ⇒
                y.read.map { y0 ⇒ (x0, y0) }
              }
            }
          }
      }

      val r0 = ST.run(p0)
      r0 should ===(5.78f, 5.77f)

      /**
       *
       */
      val p = new TaskST[(Int, Int)] {
        def apply[S] = for {
          r1 ← STReference(1)
          r2 ← STReference(2)
          x ← r1.read
          y ← r2.read
          _ ← r1.write(y + 1)
          _ ← r2.write(x + 1)
          a ← r1.read
          b ← r2.read
        } yield (a, b)
      }

      val r = ST.run(p)

      r should ===(3, 2)
    }
  }
}