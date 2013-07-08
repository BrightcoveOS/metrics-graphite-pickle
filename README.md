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
  <version>1.2.0</version>
</dependency>
```

Change Log
----------

### 1.2.0
* Stop using jython for metrics pickling. Hand-coded minimal pickling
  of the primitive data types. Jython unpickling remains in use during
  tests.

### 1.1.0
* Modify the `GraphitePickleReporter` to use `executor.scheduleAtFixedRate` rather than `executor.scheduleWithFixedDelay` (will be unnecessary after upgrading to metrics 3.0.0)
* Upgrade to metrics-core-2.2.0
* Upgrade to metrics-graphite-2.2.0
* Upgrade to jython-standalone-2.5.3
* Upgrade to slf4j 1.7.2


### 1.0.5
* Fixed a bug where a null prefix caused the last word in a metric name to be lost in some cases (e.g. 'mean')
* Upgrade to metrics-graphite-2.1.3

### 1.0.4
* Move the creation of MetricsPickler back into the run method but use a synchronized method to ensure it is only created
  once. This improves startup time by a few seconds in some cases.

### 1.0.3
* Moved the creation of the jython script engine out of the reporter's "run" method. This should fix a memory leak
  caused by ThreadLocal entries that never got cleaned up.
* Upgrade to metrics-graphite 2.1.2

### 1.0.2
* Changed encoding to ISO-8859-1 to match the python default

### 1.0.1
* Fixed a bug where a `batchSize` passed to `enable` was ignored.
* Recreates the socket on each batch to prevent reaching the 100K byte limit (and a broken pipe) 

### 1.0.0
Initial release.
