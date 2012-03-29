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
  <version>1.0.1</version>
</dependency>
```

Change Log
----------

### 1.0.2
* Changed encoding to ISO-8859-1 to match the python default

### 1.0.1
* Fixed a bug where a `batchSize` passed to `enable` was ignored.
* Recreates the socket on each batch to prevent reaching the 100K byte limit (and a broken pipe) 

### 1.0.0
Initial release.
