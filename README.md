metrics-graphite-pickle
=======

An extension to Coda Hale's excellent [Metrics library](http://metrics.codahale.com/). It 
provides `GraphitePickleReporter`, which allows your application to stream metric values 
to a [Graphite](http://graphite.wikidot.com/) server using graphite's pickle receiver. 
This is recommended if sending large amounts of data:

``` java
GraphitePickleReporter.enable(1, TimeUnit.MINUTES, "graphite.example.com", 2004);
```

### Setting up Maven

To add `metrics-graphite-pickle` to your project:

```
<dependency>
  <groupId>com.brightcove.metrics</groupId>
  <artifactId>metrics-graphite-pickle</artifactId>
  <version>1.0.0</version>
</dependency>
```
