package com.flipkart.varadhi.consumer;

import com.codahale.metrics.Timer;
import com.codahale.metrics.*;
import com.flipkart.varadhi.consumer.concurrent.Context;
import com.flipkart.varadhi.consumer.concurrent.CustomThread;
import com.flipkart.varadhi.consumer.concurrent.EventExecutor;
import com.flipkart.varadhi.consumer.impl.ConcurrencyControlImpl;
import com.flipkart.varadhi.consumer.impl.SlidingWindowThresholdProvider;
import com.flipkart.varadhi.consumer.impl.SlidingWindowThrottler;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import com.google.common.util.concurrent.RateLimiter;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ConcurrencyAndRLDemo {

    private static final MetricRegistry registry = SharedMetricRegistries.getOrCreate("default");

    public static void main(String[] args) throws Exception {
        log.info("Starting WebSocket server...");
        CountDownLatch latch = new CountDownLatch(1);
        Vertx vertx = Vertx.vertx();

        var socketVerticle = new WebSocketVerticle();
        vertx.deployVerticle(socketVerticle, ah -> {
            if (ah.succeeded()) {
                log.info("verticle deployed");
            } else {
                log.error("failed to deploy verticle", ah.cause());
                latch.countDown();
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            vertx.close();
            latch.countDown();
        }));

//        latch.await();

        Simulation.doSimulation(1000, 90.0f, 50, 20.0f, 10, 0, socketVerticle::send);
    }

    public static class Simulation {

        public static void doSimulation(
                int loadGenRate, float errorPctThreshold, int minErrorThreshold, float errorPctSimulate,
                long latencySimulate, long errorProduceLatency, Consumer<Map<String, Double>> metricListener
        ) throws Exception {
            InternalQueueType mainQ = new InternalQueueType.Main();
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            ScheduledExecutorService httpScheduler = Executors.newSingleThreadScheduledExecutor();
            ScheduledExecutorService websocketScheduler = Executors.newSingleThreadScheduledExecutor();


            EventExecutor executor =
                    new EventExecutor(null, CustomThread::new, new LinkedBlockingQueue<>());
            Context ctx = new Context(executor);
            ctx.updateCurrentThreadContext();

            ConcurrencyControl<Boolean> cc =
                    new ConcurrencyControlImpl<>(ctx, 10, new InternalQueueType[]{mainQ});
            try (
                    SlidingWindowThresholdProvider dynamicThreshold =
                            new SlidingWindowThresholdProvider(
                                    scheduler, Ticker.systemTicker(), 2_000, 1_000, errorPctThreshold);
                    SlidingWindowThrottler<Boolean> throttler =
                            new SlidingWindowThrottler<>(scheduler, Ticker.systemTicker(), minErrorThreshold, 1_000, 10,
                                    new InternalQueueType[]{mainQ}
                            );
                    var metricsFile = new PrintStream(Files.newOutputStream(Path.of("/tmp/demo_metrics")));
                    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).outputTo(metricsFile).build();
            ) {
                dynamicThreshold.addListener(newThreshold -> {
                    log.debug("threshold changed to : {}", newThreshold);
                    throttler.onThresholdChange(Math.max(newThreshold, minErrorThreshold));
                });

                RateLimiter loadGenRL = RateLimiter.create(loadGenRate);
                Meter loadGenMeter =
                        registry.register("load.gen.rate", new Meter());
                Meter errorExpMeter = registry.register("task.error.experienced.rate", new Meter());
                Timer completionLatency = registry.register(
                        "task.completion.latency",
                        new Timer(new SlidingTimeWindowArrayReservoir(60, TimeUnit.SECONDS))
                );
                Timer throttlerAcquireLatency = registry.register(
                        "throttler.acquire.latency",
                        new Timer(new SlidingTimeWindowArrayReservoir(60, TimeUnit.SECONDS))
                );
                Gauge<Float> errorThresholdGuage =
                        registry.registerGauge("error.threshold.value", dynamicThreshold::getThreshold);
                if (metricListener != null) {
                    websocketScheduler.scheduleAtFixedRate(() -> {
                        Map<String, Double> datapoints = new HashMap<>();
                        datapoints.put("error.experienced.rate", errorExpMeter.getOneMinuteRate());
                        datapoints.put("task.completion.rate", completionLatency.getOneMinuteRate());
                        datapoints.put("error.threshold.value", (double) errorThresholdGuage.getValue());
                        metricListener.accept(datapoints);
                    }, 1_000, 2_000, TimeUnit.MILLISECONDS);
                }
                reporter.start(2, TimeUnit.SECONDS);
                AtomicInteger throttlePending = new AtomicInteger(0);
                Gauge<Integer> throttleGuage = registry.registerGauge("throttle.pending", throttlePending::get);

                Random rndm = new Random();


                // workload simulation
                // concurrency : 10, latency: 10ms, qps = 1000

                AtomicInteger taskId = new AtomicInteger(0);

                class SS implements Supplier<CompletableFuture<Boolean>> {

                    int id = taskId.incrementAndGet();
                    volatile Timer.Context timerCtx = null;

                    @Override
                    public CompletableFuture<Boolean> get() {
                        // this is a single task of http call simulation
                        timerCtx = completionLatency.time();
                        CompletableFuture<Boolean> afterHttp = new CompletableFuture<>();
                        httpScheduler.schedule(() -> {
                            boolean failed = ThreadLocalRandom.current().nextFloat() * 100.0f < errorPctSimulate;
                            log.debug("task: {}, http done, failure: {}", id, failed);
                            if (failed) {
                                errorExpMeter.mark();
                            }
                            dynamicThreshold.mark();
                            afterHttp.complete(failed);
                        }, latencySimulate, TimeUnit.MILLISECONDS);

                        return afterHttp.thenCompose(failed -> {
                            if (failed) {
                                // throttled failure handling.
                                Timer.Context throttlerCtx = throttlerAcquireLatency.time();
                                throttlePending.incrementAndGet();
                                return throttler.acquire(mainQ, () -> {
                                    throttlePending.decrementAndGet();
                                    throttlerCtx.stop();
                                    log.debug("task: {}, throttle acquired", id);
                                    // And the "failure handling" is : produce to rq simulation
                                    if (errorProduceLatency > 0) {
                                        CompletableFuture<Boolean> produced = new CompletableFuture<>();
                                        httpScheduler.schedule(() -> {
                                            log.debug("task: {}, produced to rq", id);
                                            produced.complete(failed);
                                        }, errorProduceLatency, TimeUnit.MILLISECONDS);
                                        return produced;
                                    } else {
                                        // just a short-circuit for no latency.
                                        return CompletableFuture.completedFuture(failed);
                                    }
                                }, 1);
                            } else {

                                // just a short-circuit for no failure.
                                return CompletableFuture.completedFuture(failed);
                            }
                        }).whenComplete((r, e) -> {
                            timerCtx.stop();
                            log.debug("task: {}, completed with result: {}", id, r);
                        });
                    }
                }

                AtomicLong inFlightTasks = new AtomicLong(0);
                while (true) {
                    int batchSz = Math.min(rndm.nextInt(10) + 5, loadGenRate); // 5 to 15.
                    loadGenRL.acquire(batchSz);
                    loadGenMeter.mark(batchSz);
                    inFlightTasks.addAndGet(batchSz);
                    CompletableFuture<Void> taskCreationDone = new CompletableFuture<>();
                    ctx.getExecutor().execute(() -> {
                        for (CompletableFuture<Boolean> task : cc.enqueueTasks(
                                mainQ, repeat(SS::new, batchSz))) {
                            // when http call is done.
                            task.whenComplete((r, e) -> {
                                inFlightTasks.decrementAndGet();
                            });
                        }
                        taskCreationDone.complete(null);
                    });
                    taskCreationDone.join();

                    // limiting the task creation.
                    while (inFlightTasks.get() > 50_000) {
//                        log.info("inflight tasks: {}. waiting", inFlightTasks.get());
                        Thread.sleep(1_000);
                    }
                }
            }
        }
    }

    public record ClientConnection(String writeHandlerID,
                                   SockJSSocket socket,
                                   EventBus eventBus) {
        public void start() {
            socket.handler(
                    buffer -> {
                        String message = buffer.toString();
                        log.info("connection: {}, recieved: {}", writeHandlerID, message);
                        msgHandler(message).ifPresent(m -> {
                            log.info("connection: {}, sending: {}", writeHandlerID, m);
                            socket.write(m);
                        });
                    }
            );
        }

        public void send(String message) {
            log.info("connection: {}, sending: {}", writeHandlerID, message);
            socket.write(message);
        }

        Optional<String> msgHandler(String msg) {
            switch (msg) {
                case "ping":
                    return Optional.of("pong");
                case "pong":
                    log.info("Client initial handshake complete.");
                    return Optional.empty();
                default:
                    throw new IllegalStateException("Unexpected value: " + msg);
            }
        }

        void stop() {
            log.info("connection: {}, closing", writeHandlerID);
            socket.close();
        }
    }

    static class WebSocketVerticle extends AbstractVerticle {

        private Map<String, ClientConnection> connections = new ConcurrentHashMap<>();

        @Override
        public void start() {
            startServer(vertx);
        }

        private void startServer(Vertx vertx) {
            HttpServer server = vertx.createHttpServer();
            SockJSHandlerOptions options = new SockJSHandlerOptions()
                    .setHeartbeatInterval(2000)
                    .setRegisterWriteHandler(true);
            SockJSHandler ebHandler = SockJSHandler.create(vertx, options);

            Router router = Router.router(vertx);

            router.route().handler(CorsHandler.create("http://localhost:3000")
                            .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                            .allowedMethod(io.vertx.core.http.HttpMethod.POST)
                            .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
                            .allowedHeader("Access-Control-Request-Method")
                            .allowedHeader("Access-Control-Allow-Origin")
                            .allowedHeader("Access-Control-Allow-Credentials")
                            .allowedHeader("Content-Type").allowCredentials(true))
                    .handler(BodyHandler.create());
            router.route("/eventbus/*").subRouter(ebHandler.socketHandler(socket -> {
                final String id = socket.writeHandlerID();
                log.info("Client connected with id: " + id);
                ClientConnection connection = new ClientConnection(id, socket, vertx.eventBus());
                connections.put(id, connection);
                socket.endHandler((Void) -> {
                    connection.stop();
                    connections.remove(id);
                });
                connection.start();
            }));

            server.requestHandler(router)
                    .listen(5000, (ah) -> {
                        if (ah.succeeded()) {
                            log.info("server started.");
                        } else {
                            log.error("failed to start WebSocket server", ah.cause());
                        }
                    });
        }

        public void send(Map<String, Double> datapoints) {
            long ms = System.currentTimeMillis();
            StringBuilder sb = new StringBuilder("datapoint=" + "time," + ms + ",");
            sb.append(datapoints.entrySet().stream().flatMap(kv -> Stream.of(kv.getKey(), kv.getValue().toString()))
                    .collect(
                            Collectors.joining(",")));
            String msg = sb.toString();
            for (var conn : connections.values()) {
                conn.send(msg);
            }
        }
    }

    static <T> List<T> repeat(Supplier<T> obj, int times) {
        var l = new ArrayList<T>(times);
        for (int i = 0; i < times; i++) {
            l.add(obj.get());
        }
        return l;
    }
}
