package redis.netty4;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.util.Queue;

/**
 * Uses netty4 to talk to redis.
 */
public class RedisClientBase {
  private final SocketChannel socketChannel;
  private final Queue<SettableFuture<Reply>> queue;

  RedisClientBase(SocketChannel socketChannel, Queue<SettableFuture<Reply>> queue, EventLoopGroup group) {
    this.socketChannel = socketChannel;
    this.queue = queue;
    group.register(socketChannel);
  }

  public <T extends Reply> ListenableFuture<T> send(final Class<T> replyClass, final Command command) {
    final SettableFuture<Reply> replyFuture = SettableFuture.create();
    socketChannel.eventLoop().execute(new Runnable() {
      @Override
      public void run() {
        queue.add(replyFuture);
        socketChannel.writeAndFlush(command);
      }
    });
    return Futures.transform(replyFuture, new AsyncFunction<Reply, T>() {
      @Override
      @SuppressWarnings("unchecked")
      public ListenableFuture<T> apply(Reply value) throws Exception {
        if (value instanceof ErrorReply) {
          return Futures.immediateFailedFuture(new RedisException(((ErrorReply) value).data()));
        }
        if (!replyClass.isInstance(value)) {
          return Futures.immediateFailedFuture(new RedisException("Incorrect type for " + value +
                  " should be " + replyClass.getName() + " but is " + value.getClass().getName()));
        } else {
          return Futures.immediateFuture((T) value);
        }
      }
    });
  }

  public InetSocketAddress localAddress() {
    return socketChannel.localAddress();
  }

  public InetSocketAddress remoteAddress() {
    return socketChannel.remoteAddress();
  }

  public ListenableFuture<Void> close() {
    final SettableFuture<Void> future = SettableFuture.create();
    socketChannel.close().addListener(new FutureListenerAdapter<Void, ChannelFuture>(future, null));
    return future;
  }
}
