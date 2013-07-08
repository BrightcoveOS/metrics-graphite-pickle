package com.brightcove.metrics.reporting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.GraphiteReporter;
import com.yammer.metrics.reporting.SocketProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
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
        
        private String prefix;
        private SocketProvider socketProvider;
        private int batchSize;

        // graphite expects a python-pickled list of nested tuples.
        List<MetricTuple> metrics = new LinkedList<MetricTuple>();

        MetricPickler(String prefix, SocketProvider socketProvider, int batchSize) {
            this.prefix = prefix;
            this.socketProvider = socketProvider;
            this.batchSize = batchSize;
            
            LOG.debug("Created metric pickler with prefix {} and batchSize {}", prefix, batchSize);
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

            metrics.add(new MetricTuple(metricName.toString(), timestamp, value));

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
                    String payload = pickleMetrics(metrics);

                    int length = payload.length();
                    byte[] header = ByteBuffer.allocate(4).putInt(length).array();

                    Socket socket = null;
                    try {
                        socket = socketProvider.get();

                        OutputStream out = socket.getOutputStream();
                        out.write(header);

                        Writer pickleWriter = new OutputStreamWriter(out, CHARSET_NAME);
                        pickleWriter.write(payload);
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

        char 
            MARK = '(',
            STOP = '.',
            LONG = 'L',
            STRING = 'S',
            APPEND = 'a',
            LIST = 'l',
            TUPLE = 't';

        /**
         * See: http://readthedocs.org/docs/graphite/en/1.0/feeding-carbon.html
         */
        String pickleMetrics(List<MetricTuple> metrics) {
            
            StringBuilder pickled = new StringBuilder();
            pickled.append(MARK);
            pickled.append(LIST);

            for (MetricTuple tuple : metrics) {
                // start the outer tuple
                pickled.append(MARK);

                // the metric name is a string.
                pickled.append(STRING);
                // the single quotes are to match python's repr("abcd")
                pickled.append('\'');
                pickled.append(tuple.name);
                pickled.append('\'');
                pickled.append('\n');

                // start the inner tuple
                pickled.append(MARK);

                // timestamp is a long
                pickled.append(LONG);
                pickled.append(tuple.timestamp);
                // the trailing L is to match python's repr(long(1234))
                pickled.append('L');
                pickled.append('\n');

                // and the value is a string.
                pickled.append(STRING);
                pickled.append('\'');
                pickled.append(tuple.value);
                pickled.append('\'');
                pickled.append('\n');

                pickled.append(TUPLE); // inner close
                pickled.append(TUPLE); // outer close

                pickled.append(APPEND);
            }

            // every pickle ends with STOP
            pickled.append(STOP);
            return pickled.toString();
        }

        class MetricTuple {
            String name;
            long timestamp;
            String value;
            MetricTuple(String name, long timestamp, String value) {
                this.name = name;
                this.timestamp = timestamp;
                this.value = value;
            }
        }

    }
}
