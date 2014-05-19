package redis.netty4;

import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * Adapts a ChannelFutureListener to a Promise.
 */
public class ChannelFutureListenerPromiseAdapter<T> implements ChannelFutureListener {
  private final SettableFuture<T> future;
  private final T client;


  public ChannelFutureListenerPromiseAdapter(SettableFuture<T> future, T client) {
    this.future = future;
    this.client = client;
  }

  @Override
  public void operationComplete(ChannelFuture channelFuture) throws Exception {
    if (channelFuture.isSuccess()) {
      future.set(client);
    } else {
      future.setException(channelFuture.cause());
    }
  }
}
