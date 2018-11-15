package net.fabbrication.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests latency of HEAD of an s3 object, analogous to a filesystem stat().
 */
public class HeadLatencyTest extends LatencyTest {

  public static final int NUM_FILES = 20;
  public static final int FILE_SIZE = 128;

  public HeadLatencyTest(S3Properties properties) {
    super(properties, DEFAULT_ITERATIONS);
  }

  public HeadLatencyTest(S3Properties properties, int iterations) {
    super(properties, iterations);
  }

  private List<String> createFiles(int count, int bytes) {
    List<String> keys = new ArrayList(count);
    for (int i = 0; i < count; i++) {
      String key = String.format("%s/headLatencyTest/file%03d",
          props.testPath, i);
      writeFile(props.s3Bucket, key, bytes);
      keys.add(key);
    }
    return keys;
  }

  @Override
  public List<Stats> run() {

    // setup
    List<String> filesCreated = createFiles(NUM_FILES, FILE_SIZE);
    filesCreated.forEach((String key) -> { cleanupLater(key); });
    Stats stats = new Stats("HEAD latency");

    for (int i = 0; i < iterations; i++) {

        String key = filesCreated.get(i % NUM_FILES);
        Stopwatch sw = (new Stopwatch()).start();
        ObjectMetadata meta = s3Client.getObjectMetadata(props.s3Bucket, key);
        double usec = sw.elapsedTime(TimeUnit.MICROSECONDS);
        System.out.printf("HEAD of %s in %.2f msec\n", key, usec / 1000.0);
        stats.accumulate(usec);
    }

    return Collections.singletonList(stats);
  }
}
