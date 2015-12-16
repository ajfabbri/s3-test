# S3 Microbenchmarks

Beginning of some very basic S3 performance tests.

## S3 Write Latency Test

Interested in latency for small file (object) writes to S3?

1. Copy s3test.properties.example to s3test.properties and edit it.

2. Build

```
mvn package
```

3. Run

```
java -jar target/s3-test-1.0.0-jar-with-dependencies.jar
```

### Example Run (US-West)

```
[systest@fabbri-dev-1 S3Test]$ java -jar
target/s3-test-1.0.0-jar-with-dependencies.jar
# init
# run
# cleanup
# Deleted 448 objects in 19858 msec.
Write Latency 16b{min=56778.0, max=792580.0, avg=77976.03125, count=64}
Write Latency 32b{min=55529.0, max=186595.0, avg=65358.28125, count=64}
Write Latency 64b{min=53133.0, max=144272.0, avg=63641.421875, count=64}
Write Latency 128b{min=61038.0, max=213522.0, avg=76977.34375, count=64}
Write Latency 256b{min=62536.0, max=215496.0, avg=79201.875, count=64}
Write Latency 512b{min=61623.0, max=1104476.0, avg=90783.1875, count=64}
Write Latency 1024b{min=61159.0, max=217092.0, avg=73790.28125, count=64}
```



