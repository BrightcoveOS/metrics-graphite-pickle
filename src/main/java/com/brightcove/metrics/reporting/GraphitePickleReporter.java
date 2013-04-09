package com.brightcove.metrics.reporting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.reporting.GraphiteReporter;
import com.yammer.metrics.reporting.SocketProvider;
import org.python.core.PyList;
import org.python.core.PyLong;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * A simple reporter which sends out application metrics to a <a href="http://graphite.wikidot.com/faq">Graphite</a>
 * server periodically using the pickle format.
 */
public class GraphitePickleReporter extends GraphiteReporter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphitePickleReporter.class);
    final static int DEFAULT_BATCH_SIZE = 100;
    public static final String CHARSET_NAME = "ISO-8859-1";

    private final Locale locale = Locale.US;
    private MetricPickler pickler;
    private int batchSize = DEFAULT_BATCH_SIZE; // how many metrics per pickle payload?

    
    public static void enable(long period, TimeUnit unit, String host, int port) {
        enable(Metrics.defaultRegistry(), period, unit, host, port);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port) {
        enable(metricsRegistry, period, unit, host, port, null);
    }

    public static void enable(long period, TimeUnit unit, String host, int port, String prefix) {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix);
    }

    public static void enable(long period, TimeUnit unit, String host, int port, String prefix, int batchSize) {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix, batchSize);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix) {
        enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL, DEFAULT_BATCH_SIZE);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, 
            int batchSize) {
        enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL, batchSize);
    }

    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, 
            MetricPredicate predicate) {
        enable(metricsRegistry, period, unit, host, port, prefix, predicate, DEFAULT_BATCH_SIZE);
    }

    /**
     * Enables the graphite pickle reporter to send data to graphite server with the specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of graphite server (carbon-cache agent)
     * @param port            the port number on which the graphite server is listening
     * @param prefix          the string which is prepended to all metric names
     * @param predicate       filters metrics to be reported
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, 
            MetricPredicate predicate, int batchSize) {
        try {
            final GraphitePickleReporter reporter = new GraphitePickleReporter(metricsRegistry,
                                                                   prefix,
                                                                   predicate,
                                                                   new DefaultSocketProvider(host,
                                                                                             port),
                                                                   Clock.defaultClock(),
                                                                   batchSize);
            reporter.start(period, unit);
        } catch (Exception e) {
            LOG.error("Error creating/starting Graphite reporter:", e);
        }
    }

    /**
     * Creates a new {@link GraphitePickleReporter}.
     *
     * @param host   is graphite server
     * @param port   is port on which graphite server is running
     * @param prefix is prepended to all names reported to graphite
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphitePickleReporter(String host, int port, String prefix) throws IOException {
        this(Metrics.defaultRegistry(), host, port, prefix);
    }

    /**
     * Creates a new {@link GraphitePickleReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param host            is graphite server
     * @param port            is port on which graphite server is running
     * @param prefix          is prepended to all names reported to graphite
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphitePickleReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) throws IOException {
        this(metricsRegistry,
             prefix,
             MetricPredicate.ALL,
             new DefaultSocketProvider(host, port),
             Clock.defaultClock());
    }

    /**
     * Creates a new {@link GraphitePickleReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param prefix          is prepended to all names reported to graphite
     * @param predicate       filters metrics to be reported
     * @param socketProvider  a {@link SocketProvider} instance
     * @param clock           a {@link Clock} instance
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphitePickleReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock) throws IOException {
        this(metricsRegistry, prefix, predicate, socketProvider, clock,
             VirtualMachineMetrics.getInstance(), DEFAULT_BATCH_SIZE);
    }

    /**
     * Creates a new {@link GraphitePickleReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param prefix          is prepended to all names reported to graphite
     * @param predicate       filters metrics to be reported
     * @param socketProvider  a {@link SocketProvider} instance
     * @param clock           a {@link Clock} instance
     * @param batchSize       how many data points should accumulate into a single pickle message
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphitePickleReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock, int batchSize) throws IOException {
        this(metricsRegistry, prefix, predicate, socketProvider, clock,
             VirtualMachineMetrics.getInstance(), batchSize);
    }

    /**
     * Creates a new {@link GraphitePickleReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param prefix          is prepended to all names reported to graphite
     * @param predicate       filters metrics to be reported
     * @param socketProvider  a {@link SocketProvider} instance
     * @param clock           a {@link Clock} instance
     * @param virtualMachine  a {@link VirtualMachineMetrics} instance
     * @param batchSize       how many data points should accumulate into a single pickle message
     * @throws IOException if there is an error connecting to the Graphite server
     */
    public GraphitePickleReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics virtualMachine, int batchSize) throws IOException {
        super(metricsRegistry, prefix, predicate, socketProvider, clock, virtualMachine, "graphite-pickle-reporter");
        this.batchSize = batchSize;
    }

    /**
     * Create the pickler if it hasn't already been created.
     */
    private synchronized MetricPickler getPickler() {
        if (pickler == null) {
            pickler = new MetricPickler(prefix, socketProvider, batchSize);
        }
        return pickler;
    }

    @Override
    public void run() {
        try {
            if (getPickler() != null) {
                final long epoch = clock.time() / 1000;
                if (this.printVMMetrics) {
                    printVmMetrics(epoch);
                }
                printRegularMetrics(epoch);
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to Graphite", e);
            } else {
                LOG.warn("Error writing to Graphite: {}", e.getMessage());
            }
        } finally {
            if(getPickler() != null) {
                // finish writing any left over metrics
                getPickler().writeMetrics();
            }
            
        }
    }

    /**
     * Starts the reporter polling at the given period.
     *
     * @param period    the amount of time between polls
     * @param unit      the unit for {@code period}
     */
    @Override
    public void start(long period, TimeUnit unit) {
        // the parent class uses scheduleWithFixedDelay, but we really want scheduleAtFixedRate
        // try to get a hold of the executor and fall back to the parent if necessary
        // remove this when we upgrade to metrics 3.0
        ScheduledExecutorService executor = null;
        try {
            Field field = AbstractPollingReporter.class.getDeclaredField("executor");
            field.setAccessible(true);
            Object value = field.get(this);
            if (value instanceof ScheduledExecutorService) {
                executor = (ScheduledExecutorService) value;
            }
        } catch (Throwable t) {
            LOG.warn("Unable to use scheduleAtFixedRate", t);
        }
        if (executor != null) {
            LOG.debug("Using scheduleAtFixedRate instead of default");
            executor.scheduleAtFixedRate(this, period, period, unit);
        } else {
            super.start(period, unit);
        }
    }

    @Override
    protected void sendInt(long timestamp, String name, String valueName, long value) {
        getPickler().addMetric(timestamp, name, valueName, String.format(locale, "%d", value));
    }

    
    @Override
    protected void sendFloat(long timestamp, String name, String valueName, double value) {
        getPickler().addMetric(timestamp, name, valueName, String.format(locale, "%2.2f", value));
    }

    @Override
    protected void sendObjToGraphite(long timestamp, String name, String valueName, Object value) {
        getPickler().addMetric(timestamp, name, valueName, String.format(locale, "%s", value));
    }

    @SuppressWarnings("restriction")
    private class MetricPickler {
        
        /**
         * See: http://readthedocs.org/docs/graphite/en/1.0/feeding-carbon.html
         */
        private static final String PICKLER_SCRIPT = 
        		"import struct\n" +
        		"import cPickle\n" +
        		"payload = cPickle.dumps(metrics)\n" +
        		"header = struct.pack(\"!L\", len(payload))\n" +
        		"message = header + payload\n";
        
        private String prefix;
        private SocketProvider socketProvider;
        private int batchSize;
        // graphite expects a pickled list of python tuples
        PyList metrics = new PyList();

        private CompiledScript pickleScript;

        
        MetricPickler(String prefix, SocketProvider socketProvider, int batchSize) {
            this.prefix = prefix;
            this.socketProvider = socketProvider;
            this.batchSize = batchSize;
            
            LOG.debug("Created metric pickler with prefix {} and batchSize {}", prefix, batchSize);
            
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("python");            
            Compilable compilable = (Compilable) engine;
            try {
                pickleScript = compilable.compile(PICKLER_SCRIPT);
            } catch (ScriptException e) {
                throw new RuntimeException("Unable to compile pickle script", e);
            }

        }
        
        /**
         * Convert the metric to a python tuple of the form:
         * 
         *      (timestamp, (name, value))
         *      
         * And add it to the list of metrics. 
         * If we reach the batch size, write them out.
         */
        public void addMetric(long timestamp, String name, String valueName, String value) {
            StringBuilder metricName = new StringBuilder(sanitizeString(name));
            if (!prefix.isEmpty()) {
                metricName.insert(0, prefix);
            }
            metricName.append(".").append(valueName);

            PyTuple tuple = new PyTuple(new PyString(metricName.toString()),
                new PyTuple(new PyLong(timestamp), new PyString(value)));
            metrics.add(tuple);
            if(metrics.size() >= batchSize) {
                writeMetrics();
            }
        }

        /**
         * 1. Run the pickler script to package all the pending metrics into a single message
         * 2. Send the message to graphite
         * 3. Clear out the list of metrics 
         */
        private void writeMetrics() {
            if (metrics.size() > 0) {
                try {
                    Bindings bindings = new SimpleBindings();
                    bindings.put("metrics", metrics);
                    pickleScript.eval(bindings);
                    Object result = bindings.get("message");
                    String message = result.toString();

                    Socket socket = null;
                    Writer pickleWriter = null;
                    try {
                        socket = socketProvider.get();
                        pickleWriter = new OutputStreamWriter(socket.getOutputStream(), CHARSET_NAME);
                        pickleWriter.write(message);
                        pickleWriter.flush();
                    } finally {
                        if (socket != null) {
                            socket.shutdownOutput();
                            socket.close();
                        }
                    }
                } catch (Exception e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Error writing to Graphite", e);
                    } else {
                        LOG.warn("Error writing to Graphite: {}", e.getMessage());
                    }
                }
                
                // if there was an error, we might miss some data. for now, drop those on the floor and
                // try to keep going.
                LOG.debug("Wrote {} metrics", metrics.size());
                
                metrics.clear();
            }
        }        
    }
}
