package redis.netty4;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import redis.util.Encoding;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertTrue;
import static redis.netty4.RedisClientBase.connect;

/**
 * Some tests for the client.
 */
public class RedisClientTest {

  public static final long CALLS = 1000000;

  @Test
  public void testSetGet() throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicBoolean success = new AtomicBoolean();
    final AtomicBoolean matches = new AtomicBoolean();
    Futures.addCallback(connect("localhost", 6379), new FutureCallback<RedisClientBase>() {
      @Override
      public void onSuccess(final RedisClientBase client) {
        Futures.addCallback(client.send(new Command("SET", "test", "value")), new FutureCallback<Reply>() {
          @Override
          public void onSuccess(Reply reply) {
            success.set(reply.data().equals("OK"));
            Futures.addCallback(client.send(new Command("GET", "test")), new FutureCallback<Reply>() {
              @Override
              public void onSuccess(Reply reply) {
                if (reply instanceof BulkReply) {
                  matches.set(((BulkReply) reply).asAsciiString().equals("value"));
                }
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
  public void testBenchmark() throws InterruptedException {
    if (System.getenv().containsKey("CI") || System.getProperty("CI") != null) return;
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicInteger calls = new AtomicInteger(0);

    long start = System.currentTimeMillis();
    Futures.addCallback(connect("localhost", 6379), new FutureCallback<RedisClientBase>() {
      @Override
      public void onSuccess(final RedisClientBase client) {
        int i = calls.getAndIncrement();
        if (i == CALLS) {
          countDownLatch.countDown();
        } else {
          final FutureCallback<RedisClientBase> thisBenchmark = this;
          Futures.addCallback(client.send(new Command("SET", Encoding.numToBytes(i), "value")), new FutureCallback<Reply>() {
            @Override
            public void onSuccess(Reply reply) {
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
    RedisClientBase client = connect("localhost", 6379).get();
    final Semaphore semaphore = new Semaphore(100);
    for (int i = 0; i < CALLS; i++) {
      semaphore.acquire();
      client.send(new Command("SET", Encoding.numToBytes(i), "value")).addListener(new Runnable() {
        @Override
        public void run() {
          semaphore.release();
        }
      }, MoreExecutors.sameThreadExecutor());
    }
    semaphore.acquire(50);
    System.out.println("Netty4 pipelined: " + CALLS * 1000 / (System.currentTimeMillis() - start));
  }
}
