package net.fabbrication.s3;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Stopwatch;

/**
 * Created by fabbri on 12/15/15.
 */
public class WriteLatencyTest {

  S3Properties props;

  private final int iterations;
  private final int minBytes;
  private final int maxBytes;

  private final byte buffer[];
  private final List<String> filesCreated;

  private AmazonS3 s3Client;

  public final static int DEFAULT_ITERATIONS = 64;
  public final static int DEFAULT_MIN_BYTES = 16;
  public final static int DEFAULT_MAX_BYTES = 1024;

  public WriteLatencyTest(S3Properties properties) {
    this(properties, DEFAULT_ITERATIONS, DEFAULT_MIN_BYTES, DEFAULT_MAX_BYTES);
  }

  public WriteLatencyTest(S3Properties properties, int iterations,
      int minBytes, int maxBytes) {
    this.props = properties;
    this.iterations = iterations;
    this.minBytes = minBytes;
    this.maxBytes = maxBytes;

    buffer = new byte[maxBytes];
    Random random = new Random();
    random.nextBytes(buffer);

    filesCreated = new ArrayList<>();
  }

  public void init() throws Exception {
    AWSCredentials creds = new BasicAWSCredentials(props.accessKey, props.secretKey);
    s3Client = new AmazonS3Client(creds);
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
        filesCreated.add(filePath);
      }
      results.add(stats);

      size = size * 2;
      if (size > maxBytes) {
        size = maxBytes;
      }
    }
    return results;
  }

  /** Cleans up after test. */
  public void destroy() {
    Stopwatch sw = (new Stopwatch().start());
    int i = 0;
    for (String file : filesCreated) {
      s3Client.deleteObject(props.s3Bucket, file);
      i++;
    }
    System.out.printf("# Deleted %d objects in %d msec.\n", i, sw.elapsedMillis());
  }

  /** @return elapsed time, in microseconds, of writing S3 file of length 'bytes' to 'path'. */
  double writeFile(String bucket, String path, int bytes) {
    InputStream stream = new ByteArrayInputStream(buffer);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(bytes);
    // content type defaults to application/octet-stream
    PutObjectRequest req = new PutObjectRequest(bucket, path, stream, meta);
    Stopwatch sw = (new Stopwatch()).start();
    s3Client.putObject(req);
    double usec = sw.elapsedTime(TimeUnit.MICROSECONDS);
    return usec;
  }

}
