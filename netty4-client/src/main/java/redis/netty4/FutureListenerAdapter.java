package redis.netty4;

import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Adapts a GenericFutureListener to a SettableFuture.
 */
public class FutureListenerAdapter<V, F extends Future<?>> implements GenericFutureListener<F> {
  private final SettableFuture<V> settableFuture;
  private final V value;


  public FutureListenerAdapter(SettableFuture<V> future, V value) {
    this.settableFuture = future;
    this.value = value;
  }

  @Override
  public void operationComplete(Future future) throws Exception {
    if (future.isSuccess()) {
      settableFuture.set(value);
    } else {
      settableFuture.setException(future.cause());
    }
  }
}
