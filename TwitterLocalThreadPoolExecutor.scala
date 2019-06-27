import com.twitter.util.Local
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit}
import slick.util.AsyncExecutor.PrioritizedRunnable

// This class wraps the slick.util.AsyncExecutor.PrioritizedRunnable and implements
// a `run` method to pass Finagle Context from one thread to another during a
// context switch.  This approach is adapted from Finagle FuturePool thread switching
// implementation
// https://github.com/twitter/util/blob/86ab24f5/util-core/src/main/scala/com/twitter/util/FuturePool.scala#L125-L151
class PrioritizedRunnableWrapper(
  context: Local.Context,
  underlying: PrioritizedRunnable
) extends PrioritizedRunnable {

  // copy underlying member fields into this wrapper
  def priority = underlying.priority
  connectionReleased = underlying.connectionReleased
  inUseCounterSet = underlying.inUseCounterSet

  override def run(): Unit = {
    val current = Local.save()
    Local.restore(context)
    try {
      underlying.run
      // underlying.run will change the state of these vars during execution.
      // Thus, we need to update their values
      connectionReleased = underlying.connectionReleased
      inUseCounterSet = underlying.inUseCounterSet
    } finally {
      Local.restore(current)
    }
  }

}

class TwitterLocalThreadPoolExecutor(executor: ThreadPoolExecutor) extends ThreadPoolExecutor(
  executor.getCorePoolSize,
  executor.getMaximumPoolSize,
  executor.getKeepAliveTime(TimeUnit.MILLISECONDS),
  TimeUnit.MILLISECONDS,
  executor.getQueue,
  executor.getThreadFactory
) {

  override def execute(underlying: Runnable): Unit = {
    val runnable = new PrioritizedRunnableWrapper(
      Local.save(),
      underlying.asInstanceOf[PrioritizedRunnable]
    )

    super.execute(runnable)
  }
}
