package futures

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

object Implicits {
  import scala.concurrent.Future
  import scala.concurrent.duration.Duration

  implicit class RichFutures[T](val inner: Future[T]) extends AnyVal {
    private def timeout(t: Long): Future[Boolean] = {
      import java.util._
      val timer = new Timer(true)
      val p = scala.concurrent.Promise[Boolean]()
      timer.schedule(new TimerTask {
        override def run() = {
          p.success(false)
          timer.cancel()
        }
      }, t)
      p.future
    }

    def withTimeoutFail(implicit duration: Duration, ex: ExecutionContext): Future[T] = {
      Future.firstCompletedOf(Seq(inner, timeout(duration.toMillis))).map {
        case false     ⇒ throw new Exception("IO timeout")
        case result: T ⇒ result
      }
    }

    def withFallbackStrategy(fallBackAction: ⇒ Future[T])(implicit ex: ExecutionContext): Future[T] =
      inner recoverWith {
        case e: Throwable ⇒ {
          println(e.getMessage)
          fallBackAction recoverWith { case _ ⇒ inner }
        }
      }

    def withLatencyLogging(name: String)(implicit ex: ExecutionContext): Future[T] = {
      val startTime = System.currentTimeMillis
      inner map { r ⇒
        val latency = System.currentTimeMillis - startTime
        println(s"Future $name took $latency ms to process")
        r
      }
    }
  }

  private def retry[T](n: Int)(f: ⇒ Future[T])(implicit duration: Duration, ex: ExecutionContext): Future[T] = {
    if (n > 0) f.withTimeoutFail.withFallbackStrategy(retry(n - 1)(f))
    else Future.successful[T](null.asInstanceOf[T])
  }

  def executeWithRetry[T](n: Int)(f: ⇒ Future[T])(implicit duration: Duration, ex: ExecutionContext) = retry(n)(f)

}
