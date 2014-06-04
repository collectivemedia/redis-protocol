package redis.netty4;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.*;
import com.pholser.junit.quickcheck.ForAll;
import org.junit.After;
import org.junit.contrib.theories.Theories;
import org.junit.contrib.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;


@RunWith(Theories.class)
public class RedisClientTest {
  private final RedisClientFactory factory = new RedisClientFactory();

  @After
  public void closeFactory() throws Exception {
    factory.shutdown(10, 5000, MILLISECONDS).get();
  }

  @Theory
  public void testHSetGet(@ForAll final Map<Integer, Integer> map) throws Throwable {
    final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    final RedisClient client = factory.connect("localhost", 6379).get();
    final List<ListenableFuture> futures = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      final String key = "test-" + i;
      final ListenableFuture<?> future = executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            for (int j = 0; j < 5; j++) {
              Futures.transform(client.hkeys(key), new AsyncFunction<MultiBulkReply, IntegerReply>() {
                @Override
                public ListenableFuture<IntegerReply> apply(MultiBulkReply multiBulkReply) throws Exception {
                  if (multiBulkReply.data().length > 0) {
                    return client.hdel(key, multiBulkReply.data());
                  } else {
                    return Futures.immediateFuture(new IntegerReply(0));
                  }
                }
              }).get();
              assertEquals(ImmutableMap.<String, String>of(), client.hgetall(key).get().asStringMap(UTF_8));
              Map<String, String> expected = new HashMap<>();
              for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                final String key1 = key + "-" + j + "-" + entry.getKey();
                final String value1 = key + "-" + j + "-" + entry.getValue();
                assertEquals(1, (long) client.hset(key, key1, value1).get().data());
                expected.put(key1, value1);
                assertEquals(expected, client.hgetall(key).get().asStringMap(UTF_8));
              }
            }
          } catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      });
      futures.add(future);
    }
    try {
      for (ListenableFuture future : futures) {
        future.get();
      }
    } catch (ExecutionException e) {
      throw e.getCause();
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(10, SECONDS);
      client.close().get();
    }
  }
}
