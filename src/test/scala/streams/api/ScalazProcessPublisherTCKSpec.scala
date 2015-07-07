/*
package streams.api

import org.reactivestreams.Publisher
import org.scalatest.testng.TestNGSuiteLike
import java.util.concurrent.ExecutorService
import scala.concurrent.forkjoin.ForkJoinPool
import org.reactivestreams.tck.{ PublisherVerification, TestEnvironment }

class ScalazProcessPublisherTCKSpec(env: TestEnvironment, publisherShutdownTimeout: Long)
    extends PublisherVerification[Int](env, publisherShutdownTimeout)
    with TestNGSuiteLike {

  import Procesess._

  implicit val E: ExecutorService =
    new ForkJoinPool(Runtime.getRuntime.availableProcessors() * 2)

  def this() {
    this(new TestEnvironment(700), 1000)
  }

  override def createPublisher(bound: Long): Publisher[Int] =
    ScalazProcessPublisher.bounded(naturals, bound)

  override def createFailedPublisher(): Publisher[Int] =
    ScalazProcessPublisher.failedOnSubscribe(naturals)

  override def maxElementsFromPublisher = Int.MaxValue
}*/
