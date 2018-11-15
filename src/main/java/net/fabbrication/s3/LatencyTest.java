package net.fabbrication.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Stopwatch;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Simple base class for latency-measuring tests.
 */
public abstract class LatencyTest {
  public final static int DEFAULT_ITERATIONS = 64;
  protected final int iterations;
  protected AmazonS3 s3Client;
  protected S3Properties props;
  private byte[] buffer;
  private List<String> filesCreated;
  private Random random;

  public LatencyTest(S3Properties properties, int iterations) {
    this.props = properties;
    this.iterations = iterations;
    filesCreated = new ArrayList<>();
    random = new Random();
  }

  public void init() throws Exception {
    AWSCredentials creds = new BasicAWSCredentials(props.accessKey, props.secretKey);
    s3Client = new AmazonS3Client(creds);
  }

  public abstract List<Stats> run();

  /**
   * Lazily create buffer of given size if it doesn't already exist.
   * @param bytes size
   */
  private void allocateBuffer(int bytes) {
    if (buffer == null || buffer.length != bytes) {
      buffer = new byte[bytes];
      random.nextBytes(buffer);
    }
  }

  /**
   * Queue up an object path for deletion when {@link #destroy()} is called.
   * @param key name of object (s3 file) to delete.
   */
  protected void cleanupLater(String key) {
    filesCreated.add(key);
  }

  /** @return elapsed time, in microseconds, of writing S3 file of length 'bytes' to 'path'. */
  protected double writeFile(String bucket, String path, int bytes) {
    allocateBuffer(bytes);
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

  /**
   * Cleans up after test. Base classes may extend and add more logic.
   */
  public void destroy() {
    Stopwatch sw = (new Stopwatch().start());
    int i = 0;
    for (String file : filesCreated) {
      s3Client.deleteObject(props.s3Bucket, file);
      i++;
    }
    System.out.printf("# Deleted %d objects in %d msec.\n", i, sw.elapsedMillis());
  }
}
