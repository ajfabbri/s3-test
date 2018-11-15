package net.fabbrication.s3;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by fabbri on 12/15/15.
 */
public class WriteLatencyTest extends LatencyTest {

  private final int minBytes;
  private final int maxBytes;

  public final static int DEFAULT_MIN_BYTES = 16;
  public final static int DEFAULT_MAX_BYTES = 1024;

  public WriteLatencyTest(S3Properties properties) {
    this(properties, DEFAULT_ITERATIONS, DEFAULT_MIN_BYTES, DEFAULT_MAX_BYTES);
  }

  public WriteLatencyTest(S3Properties properties, int iterations,
      int minBytes, int maxBytes) {
    super(properties, iterations);
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;

  }

  public List<Stats> run() {
    List<Stats> results = new ArrayList<>();
    boolean done = false;
    int size = minBytes;
    while (!done) {

      if (size == maxBytes) {
        done = true;
      }

      Stats stats = new Stats(String.format("Write Latency %db", size));
      for (int i = 0; i < iterations; i++) {
        String filePath = String.format("%s/wl-%05d-i%09d", props.testPath, size, i);
        stats.accumulate(writeFile(props.s3Bucket, filePath, size));
        cleanupLater(filePath);
      }
      results.add(stats);

      size = size * 2;
      if (size > maxBytes) {
        size = maxBytes;
      }
    }
    return results;
  }

}
