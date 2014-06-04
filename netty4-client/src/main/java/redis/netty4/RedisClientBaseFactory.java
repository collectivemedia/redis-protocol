package redis.netty4;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RedisClientBaseFactory {
  private final EventLoopGroup group = new NioEventLoopGroup();

  public ListenableFuture<RedisClientBase> connect(String host, int port) {
    final Queue<SettableFuture<Reply>> queue = new LinkedList<>();
    SocketChannel socketChannel = new NioSocketChannel();
    final RedisClientBase client = new RedisClientBase(socketChannel, queue, group);
    socketChannel.pipeline().addLast(new RedisCommandEncoder(), new RedisReplyDecoder(),
            new SimpleChannelInboundHandler<Reply<?>>() {
              @Override
              protected void channelRead0(ChannelHandlerContext channelHandlerContext, Reply<?> reply) throws Exception {
                final SettableFuture<Reply> future = queue.poll();
                if (future == null) {
                  throw new IllegalStateException("Promise queue is empty, received reply: " + reply);
                }
                future.set(reply);
              }
            }
    );
    final SettableFuture<RedisClientBase> future = SettableFuture.create();
    final ChannelFuture connect = socketChannel.connect(new InetSocketAddress(host, port));
    connect.addListener(new FutureListenerAdapter<RedisClientBase, ChannelFuture>(future, client));
    return future;
  }

  public ListenableFuture<Void> shutdown() {
    return shutdown(500, 5000, MILLISECONDS);
  }

  @SuppressWarnings("unchecked")
  public ListenableFuture<Void> shutdown(long quietPeriod, long timeout, TimeUnit unit) {
    final SettableFuture<Void> future = SettableFuture.create();
    final Future shutdownFuture = group.shutdownGracefully(quietPeriod, timeout, unit);
    shutdownFuture.addListener(new FutureListenerAdapter<>(future, null));
    return future;
  }
}
