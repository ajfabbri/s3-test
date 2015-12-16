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


