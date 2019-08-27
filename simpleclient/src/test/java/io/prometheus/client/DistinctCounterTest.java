package io.prometheus.client;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DistinctCounterTest {
  CollectorRegistry registry;
  DistinctCounter counter;

  int logSize = 12;
  int size = 1 << logSize;

  @Before
  public void setUp() {
    registry = new CollectorRegistry();
    counter = DistinctCounter.build().name("nolabels").help("help").logSize(logSize).register(registry);
  }

  private byte[] getValue(String metricName, int size) {
    byte[] buckets = new byte[size];
    Map<Map<String, String>, Double> metrics
            = registry.getSampleValues(metricName, new String[]{"m"}, new String[]{"*"});
    for (Map.Entry<Map<String, String>, Double> metric : metrics.entrySet()) {
      String m = metric.getKey().get("m");
      buckets[Integer.parseInt(m)] = (byte) Math.round(metric.getValue());
    }
    return buckets;
  }

  private void observe(DistinctCounter counter, String observation) {
    counter.observe(observation.getBytes(StandardCharsets.UTF_8));
  }

  private void observeN(DistinctCounter counter, int amount, String observation) {
    for (int i = 0; i < amount; i++) {
      observe(counter, observation + i);
    }
  }

  @Test
  public void sparseObservations() {
    observe(counter,"one");
    assertEquals(1.0, countMetric("nolabels", size), 0.1);
    observe(counter,"two");
    assertEquals(2.0, countMetric("nolabels", size), 0.1);
    observe(counter,"three");
    assertEquals(3.0, countMetric("nolabels", size), 0.1);
    observe(counter,"four");
    assertEquals(4.0, countMetric("nolabels", size), 0.1);
  }

  @Test
  public void denseObservations() {
    observeN(counter, 100000, "one");
    assertEquals(100000.0, countMetric("nolabels", size), 1000.0);
    observeN(counter, 100000, "two");
    assertEquals(200000.0, countMetric("nolabels", size), 2000.0);
    observeN(counter, 100000, "three");
    assertEquals(300000.0, countMetric("nolabels", size), 3000.0);
  }

  @Test
  public void repeatedObservations() {
    observeN(counter, 10000, "one");
    assertEquals(10000.0, countMetric("nolabels", size), 150);
    observeN(counter, 10000, "one");
    assertEquals(10000.0, countMetric("nolabels", size), 150);
  }

  private double countMetric(String metricName, int size) {
    byte[] buckets = getValue(metricName, size);
    return count(buckets);
  }

  private double count(byte[] buckets) {
    int v = countZeroes(buckets);
    if (v > 0) {
      return buckets.length * Math.log((1.0 * buckets.length) / v);
    }
    double mZ = arithmeticMean(buckets);
    double am = alphaApproximation(buckets.length);
    return am * buckets.length * mZ;
  }

  private int countZeroes(byte[] buckets) {
    int v = 0;
    for (int i = 0; i < buckets.length; i++) {
      if (buckets[i] == 0) {
        v++;
      }
    }
    return v;
  }

  private static double alphaApproximation(int length) {
    if (length <= 64) {
      if (length <= 16) {
        return 0.673;
      } else if (length <= 32) {
        return 0.697;
      } else {
        return 0.709;
      }
    } else {
      return 0.7213 / (1 + (1.079 / length));
    }
  }

  private static double arithmeticMean(byte[] buckets) {
    double mean = 0;
    for (byte b : buckets) {
      mean += Math.pow(2, -b);
    }
    return buckets.length / mean;
  }
}
