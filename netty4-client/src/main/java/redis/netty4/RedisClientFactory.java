package redis.netty4;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

public class RedisClientFactory {
  private final RedisClientBaseFactory factory = new RedisClientBaseFactory();

  public ListenableFuture<RedisClient> connect(String host, int port) {
    final ListenableFuture<RedisClientBase> redisClientBaseListenableFuture = factory.connect(host, port);
    return Futures.transform(redisClientBaseListenableFuture, new Function<RedisClientBase, RedisClient>() {
      @Override
      public RedisClient apply(RedisClientBase redisClientBase) {
        return new RedisClient(redisClientBase);
      }
    });
  }

  public ListenableFuture<Void> shutdown() {
    return factory.shutdown();
  }

  public ListenableFuture<Void> shutdown(long quietPeriod, long timeout, TimeUnit unit) {
    return factory.shutdown(quietPeriod, timeout, unit);
  }
}
