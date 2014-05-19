package redis.netty4;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Uses netty4 to talk to redis.
 */
public class RedisClientBase {

  private final static NioEventLoopGroup group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
  private final SocketChannel socketChannel;
  private final Queue<SettableFuture<Reply>> queue;

  protected RedisClientBase(SocketChannel socketChannel, Queue<SettableFuture<Reply>> queue) {
    this.socketChannel = socketChannel;
    this.queue = queue;
    group.register(socketChannel);
  }

  public static ListenableFuture<RedisClientBase> connect(String host, int port) {
    final Queue<SettableFuture<Reply>> queue = new LinkedList<>();
    SocketChannel socketChannel = new NioSocketChannel();
    final RedisClientBase client = new RedisClientBase(socketChannel, queue);
    socketChannel.pipeline().addLast(new RedisCommandEncoder(), new RedisReplyDecoder(),
            new SimpleChannelInboundHandler<Reply<?>>() {
              @Override
              protected void channelRead0(ChannelHandlerContext channelHandlerContext, Reply<?> reply) throws Exception {
                SettableFuture<Reply> poll;
                synchronized (client) {
                  poll = queue.poll();
                  if (poll == null) {
                    throw new IllegalStateException("Promise queue is empty, received reply");
                  }
                }
                poll.set(reply);
              }
            });
    final SettableFuture<RedisClientBase> future = SettableFuture.create();
    socketChannel.connect(new InetSocketAddress(host, port)).addListener(new ChannelFutureListenerPromiseAdapter<>(future, client));
    return future;
  }

  public ListenableFuture<Reply> send(Command command) {
    SettableFuture<Reply> reply = SettableFuture.create();
    synchronized (this) {
      queue.add(reply);
      socketChannel.writeAndFlush(command);
    }
    return reply;
  }
}
