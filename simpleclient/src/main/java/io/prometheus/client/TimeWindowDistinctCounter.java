package io.prometheus.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Maintains a ring buffer of byte[] to provide hyper log log over a sliding windows of time.
 */
class TimeWindowDistinctCounter {
  private final ReadWriteLock ringBufferLock = new ReentrantReadWriteLock();

  private final int size;
  private final byte[][] ringBuffer;
  private int currentBucket;
  private long lastRotateTimestampMillis;
  private final long durationBetweenRotatesMillis;

  public TimeWindowDistinctCounter(int size, long maxAgeSeconds, int ageBuckets) {
    this.size = size;
    this.ringBuffer = new byte[ageBuckets][size];
    for (int i = 0; i < ageBuckets; i++) {
      this.ringBuffer[i] = new byte[size];
    }
    this.currentBucket = 0;
    this.lastRotateTimestampMillis = System.currentTimeMillis();
    this.durationBetweenRotatesMillis = TimeUnit.SECONDS.toMillis(maxAgeSeconds) / ageBuckets;
  }

  public byte[] get() {
    Lock bLock = ringBufferLock.writeLock();
    bLock.lock();
    try {
      byte[] currentBucket = rotate();
      return currentBucket.clone();
    } finally {
      bLock.unlock();
    }
  }

  public void incrementMax(int m, byte max) {
    Lock bLock = ringBufferLock.writeLock();
    bLock.lock();
    try {
      rotate();
      for (byte[] buffer : ringBuffer) {
        if (max > buffer[m]) {
          buffer[m] = max;
        }
      }
    } finally {
      bLock.unlock();
    }
  }

  private byte[] rotate() {
    long timeSinceLastRotateMillis = System.currentTimeMillis() - lastRotateTimestampMillis;
    while (timeSinceLastRotateMillis > durationBetweenRotatesMillis) {
      ringBuffer[currentBucket] = new byte[size];
      if (++currentBucket >= ringBuffer.length) {
        currentBucket = 0;
      }
      timeSinceLastRotateMillis -= durationBetweenRotatesMillis;
      lastRotateTimestampMillis += durationBetweenRotatesMillis;
    }
    return ringBuffer[currentBucket];
  }
}
