package net.fabbrication.s3;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class Main {

  public static final String CONFIG_FILENAME = "s3test.properties";
  public static final String ACCESS_KEY = "access-key";
  public static final String SECRET_KEY = "secret-key";
  public static final String BUCKET = "bucket";
  public static final String TEST_PATH = "test-path";


  private static S3Properties getProperties() {
    Properties p =  new Properties();
    try {
      InputStream input = new FileInputStream(CONFIG_FILENAME);
      p.load(input);
    } catch (IOException ex) {
      throw new RuntimeException("Couldn't read " + CONFIG_FILENAME, ex);
    }

    S3Properties s3Properties = new S3Properties();
    s3Properties.accessKey = p.getProperty(ACCESS_KEY);
    s3Properties.secretKey = p.getProperty(SECRET_KEY);
    s3Properties.s3Bucket = p.getProperty(BUCKET);
    s3Properties.testPath = p.getProperty(TEST_PATH);
    if (s3Properties.accessKey == null || s3Properties.secretKey == null || s3Properties.s3Bucket == null
        || s3Properties.testPath == null)
    {
      throw new RuntimeException(CONFIG_FILENAME + " is missing a property.");
    }
    return s3Properties;
  }

  public static void main(String[] args) {
    LatencyTest tests[] = {new WriteLatencyTest(getProperties()),
        new HeadLatencyTest(getProperties())};


    for (LatencyTest test : tests) {
      System.out.printf("=====> Starting %s <======\n",
          test.getClass().getSimpleName());

      List<Stats> results = null;
      try {
        System.out.println("# init");
        test.init();
        System.out.println("# run");
        results = test.run();
      } catch (Exception ex) {
        throw new RuntimeException("Fail.", ex);
      } finally {
        System.out.println("# cleanup");
        test.destroy();
      }
      for (Stats s : results) {
        System.out.println(s.toString());
      }
    }
  }
}
