package redis.netty4;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.junit.After;
import org.junit.Test;
import redis.util.Encoding;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Some tests for the client.
 */
public class RedisClientBaseTest {

  private static final long CALLS = 1000000;

  private final RedisClientBaseFactory factory = new RedisClientBaseFactory();

  @After
  public void closeFactory() {
    factory.shutdown();
  }

  @Test
  public void testSetGet() throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicBoolean success = new AtomicBoolean();
    final AtomicBoolean matches = new AtomicBoolean();
    Futures.addCallback(factory.connect("localhost", 6379), new FutureCallback<RedisClientBase>() {
      @Override
      public void onSuccess(final RedisClientBase client) {
        Futures.addCallback(client.send(StatusReply.class, new Command("SET", "test", "value")), new FutureCallback<StatusReply>() {
          @Override
          public void onSuccess(StatusReply reply) {
            success.set(reply.data().equals("OK"));
            Futures.addCallback(client.send(BulkReply.class, new Command("GET", "test")), new FutureCallback<BulkReply>() {
              @Override
              public void onSuccess(BulkReply reply) {
                matches.set(reply.asAsciiString().equals("value"));
                countDownLatch.countDown();
              }

              @Override
              public void onFailure(Throwable t) {

              }
            });
          }

          @Override
          public void onFailure(Throwable t) {

          }
        });
      }

      @Override
      public void onFailure(Throwable t) {
        countDownLatch.countDown();
      }
    });
    countDownLatch.await();
    assertTrue(success.get());
    assertTrue(matches.get());
  }

  @Test
  public void testHSetGet() throws InterruptedException, ExecutionException {
    final RedisClientBase client = factory.connect("localhost", 6379).get();
    client.send(IntegerReply.class, new Command("HDEL", "htest", "field1", "field2")).get();
    assertEquals(ImmutableMap.<String, String>of(),
            client.send(MultiBulkReply.class, new Command("HGETALL", "htest")).get().asStringMap(UTF_8));
    assertEquals(1, (long) client.send(IntegerReply.class, new Command("HSET", "htest", "field1", "value1")).get().data());
    assertEquals(ImmutableMap.of("field1", "value1"),
            client.send(MultiBulkReply.class, new Command("HGETALL", "htest")).get().asStringMap(UTF_8));
    assertEquals(1, (long) client.send(IntegerReply.class, new Command("HSET", "htest", "field2", "value2")).get().data());
    assertEquals(ImmutableMap.of("field1", "value1", "field2", "value2"),
            client.send(MultiBulkReply.class, new Command("HGETALL", "htest")).get().asStringMap(UTF_8));
  }

  @Test
  public void testBenchmark() throws InterruptedException {
    if (System.getenv().containsKey("CI") || System.getProperty("CI") != null) return;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicInteger calls = new AtomicInteger(0);

    long start = System.currentTimeMillis();
    Futures.addCallback(factory.connect("localhost", 6379), new FutureCallback<RedisClientBase>() {
      @Override
      public void onSuccess(final RedisClientBase client) {
        int i = calls.getAndIncrement();
        if (i == CALLS) {
          countDownLatch.countDown();
        } else {
          final FutureCallback<RedisClientBase> thisBenchmark = this;
          Futures.addCallback(client.send(StatusReply.class, new Command("SET", Encoding.numToBytes(i), "value")), new FutureCallback<StatusReply>() {
            @Override
            public void onSuccess(StatusReply reply) {
              thisBenchmark.onSuccess(client);
            }

            @Override
            public void onFailure(Throwable t) {

            }
          });
        }
      }

      @Override
      public void onFailure(Throwable t) {
        countDownLatch.countDown();
      }
    });
    countDownLatch.await();
    System.out.println("Netty4: " + CALLS * 1000 / (System.currentTimeMillis() - start));
  }

  @Test
  public void testPipelinedBenchmark() throws ExecutionException, InterruptedException {
    if (System.getenv().containsKey("CI") || System.getProperty("CI") != null) return;
    long start = System.currentTimeMillis();
    RedisClientBase client = factory.connect("localhost", 6379).get();
    final Semaphore semaphore = new Semaphore(100);
    for (int i = 0; i < CALLS; i++) {
      semaphore.acquire();
      Futures.addCallback(client.send(StatusReply.class, new Command("SET", Encoding.numToBytes(i), "value")), new FutureCallback<StatusReply>() {
        @Override
        public void onSuccess(StatusReply result) {
          if (!"OK".equals(result.data())) {
            System.err.println(result.data());
          }
          semaphore.release();
        }

        @Override
        public void onFailure(Throwable t) {
          t.printStackTrace();
        }
      });
    }
    semaphore.acquire(50);
    System.out.println("Netty4 pipelined: " + CALLS * 1000 / (System.currentTimeMillis() - start));
  }
}
