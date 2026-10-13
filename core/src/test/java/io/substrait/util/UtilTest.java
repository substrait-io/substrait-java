package io.substrait.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class UtilTest {

  @Test
  void memoizeComputesOnce() {
    AtomicInteger calls = new AtomicInteger();
    Supplier<Object> memoized = Util.memoize(() -> calls.incrementAndGet());

    assertEquals(1, memoized.get());
    assertEquals(1, memoized.get());
    assertEquals(1, calls.get());
  }

  @Test
  void memoizeCachesNull() {
    AtomicInteger calls = new AtomicInteger();
    Supplier<Object> memoized =
        Util.memoize(
            () -> {
              calls.incrementAndGet();
              return null;
            });

    memoized.get();
    memoized.get();
    assertEquals(1, calls.get());
  }

  @Test
  void memoizeComputesOnceUnderConcurrentFirstAccess() throws Exception {
    int threads = 8;
    AtomicInteger calls = new AtomicInteger();
    // Holds the delegate open until every thread has entered it, so an unsynchronized memoizer
    // lets all of them through; a synchronized one admits a single caller, whose wait expires.
    CountDownLatch entered = new CountDownLatch(threads);
    Supplier<Object> memoized =
        Util.memoize(
            () -> {
              calls.incrementAndGet();
              entered.countDown();
              try {
                entered.await(200, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return new Object();
            });

    CyclicBarrier start = new CyclicBarrier(threads);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<Object>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  return memoized.get();
                }));
      }

      Object first = results.get(0).get(10, TimeUnit.SECONDS);
      for (Future<Object> result : results) {
        assertSame(first, result.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, calls.get());
    } finally {
      executor.shutdownNow();
    }
  }
}
