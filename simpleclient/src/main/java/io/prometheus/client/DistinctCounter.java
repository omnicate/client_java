package io.prometheus.client;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A counter of distinct observations. Implements hyper log log counting over a (almost) sliding time window provided
 * by TimeWindowDistinctCounter.
 */
public class DistinctCounter extends SimpleCollector<DistinctCounter.Child> implements Counter.Describable {
  private final HashFunction hashFunction = Hashing.sipHash24();
  private final int logSize;
  private final long maxAgeSeconds;
  private final int ageBuckets;

  DistinctCounter(Builder b) {
    super(b);
    logSize = b.logSize;
    maxAgeSeconds = b.maxAgeSeconds;
    ageBuckets = b.ageBuckets;
    initializeNoLabelsChild();
  }

  public static class Builder extends SimpleCollector.Builder<Builder, DistinctCounter> {
    int logSize = 8;
    long maxAgeSeconds = 60;
    int ageBuckets = 4;

    /**
     * The log size of the hyper log log.
     *
     * @param size
     * @return
     */
    public Builder logSize(int size) {
      if (logSize < 4 || logSize > 31) {
        throw new IllegalArgumentException("Log size must be within 4 and 31");
      }
      logSize = size;
      return this;
    }

    /**
     * The age of the sliding time window
     * @param maxAgeSeconds
     * @return
     */
    public Builder maxAgeSeconds(long maxAgeSeconds) {
      if (maxAgeSeconds <= 0) {
        throw new IllegalArgumentException("maxAgeSeconds cannot be " + maxAgeSeconds);
      }
      this.maxAgeSeconds = maxAgeSeconds;
      return this;
    }

    /**
     *
     * @param ageBuckets
     * @return
     */
    public Builder ageBuckets(int ageBuckets) {
      if (ageBuckets <= 0) {
        throw new IllegalArgumentException("ageBuckets cannot be " + ageBuckets);
      }
      this.ageBuckets = ageBuckets;
      return this;
    }

    @Override
    public DistinctCounter create() {
      this.dontInitializeNoLabelsChild = true;
      return new DistinctCounter(this);
    }
  }

  /**
   *  Return a Builder to allow configuration of a new Counter. Ensures required fields are provided.
   *
   *  @param name The name of the metric
   *  @param help The help string of the metric
   */
  public static Builder build(String name, String help) {
    return new Builder().name(name).help(help);
  }

  /**
   *  Return a Builder to allow configuration of a new Counter.
   */
  public static Builder build() {
    return new Builder();
  }

  @Override
  protected Child newChild() {
    return new Child(hashFunction, logSize, maxAgeSeconds, ageBuckets);
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>(children.size());
    for(Map.Entry<List<String>, Child> c: children.entrySet()) {
      byte[] buckets = c.getValue().get();
      List<String> labelNamesWithM = new ArrayList<String>(labelNames);
      labelNamesWithM.add("m");
      for (int m = 0; m < buckets.length; m++) {
        List<String> labelValuesWithM = new ArrayList<String>(c.getKey());
        labelValuesWithM.add(Integer.toString(m));
        samples.add(new MetricFamilySamples.Sample(fullname, labelNamesWithM, labelValuesWithM, buckets[m]));
      }
    }
    return familySamplesList(Type.GAUGE, samples);
  }

  @Override
  public List<MetricFamilySamples> describe() {
    return Collections.<MetricFamilySamples>singletonList(new CounterMetricFamily(fullname, help, labelNames));
  }

  public void observe(byte[] data) {
    noLabelsChild.observe(data);
  }

  public static class Child {
    final HashFunction h;
    final int logSize;
    final int m;
    final TimeWindowDistinctCounter buckets;

    private Child(HashFunction h, int logSize, long maxAgeSeconds, int ageBuckets) {
      this.h = h;
      this.logSize = logSize;
      m = 1 << logSize;
      buckets = new TimeWindowDistinctCounter(m, maxAgeSeconds, ageBuckets);
    }

    public void observe(byte[] v) {
      long hash = h.hashBytes(v).asLong();
      //int indexBits = (int) (hash >>> (64 - logSize));
      //long hashBits = (-1 >>> logSize) & hash;
      int indexBits = (int)(hash & ((1 << logSize) - 1));
      long hashBits = hash >>> logSize;
      byte leadingZeroes = (byte) Long.numberOfLeadingZeros(hashBits);
      buckets.incrementMax(indexBits, leadingZeroes);
    }

    public byte[] get() {
      return buckets.get();
    }
  }
}
