package com.flipkart.varadhi.controller.qos;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.MockTicker;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.SuppressionManager;
import com.flipkart.varadhi.controller.TopicLimitService;
import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;
import com.flipkart.varadhi.qos.entity.SuppressionFactor;
import com.flipkart.varadhi.qos.entity.TopicLoadInfo;
import com.flipkart.varadhi.utils.FutureUtil;
import com.flipkart.varadhi.utils.HostUtils;
import com.flipkart.varadhi.verticles.webserver.RateLimiterService;
import com.flipkart.varadhi.verticles.webserver.TrafficSender;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.flipkart.varadhi.VaradhiApplication.readConfiguration;
import static org.mockito.Mockito.when;

@Slf4j
public class RateLimiterServiceTest {

    //    private RateLimiterService rateLimiterService;
    private static MeterRegistry meterRegistry;
    private SuppressionManager suppressionManager;
    private MockTicker ticker;
    @Mock
    private TopicLimitService topicLimitService;

    @BeforeAll
    public static void setupMetrics() {
        String[] args = {"src/test/resources/testConfiguration.yml"};
        AppConfiguration configuration = readConfiguration(args);
//        CoreServices services = new CoreServices(configuration);
        CoreServices.ObservabilityStack observabilityStack = new CoreServices.ObservabilityStack(configuration);
        meterRegistry = observabilityStack.getMeterRegistry();
    }

    @BeforeEach
    public void setUpController() throws UnknownHostException {
        MockitoAnnotations.openMocks(this); // Initialize mocks

        //setup controller side of things
        ticker = new MockTicker(System.currentTimeMillis());
        suppressionManager = new SuppressionManager(10, topicLimitService, ticker);
    }

    private static Stream<Arguments> provideRateLimitTestCSVPaths() {
        return Stream.of(
                Arguments.of("src/test/resources/simulation_profiles/test_load1.profile")
//              todo(rl): fix these profiles
//                Arguments.of("src/test/resources/simulation_profiles/test_load.profile")
//                Arguments.of("src/test/resources/simulation_profiles/single_client_single_topic_test"),
//                Arguments.of("src/test/resources/simulation_profiles/single_client_multi_topic_test")
        );
    }

    @ParameterizedTest
    @MethodSource("provideRateLimitTestCSVPaths")
    public void clientLoadSimulation(String filePath) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;
        List<Thread> threads = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Map<String, RateLimiterService> clientRateLimiterMap = new HashMap<>();
        // data structure that stores client -> topic -> (throughput, qps, duration, allowedBytes, rejectedBytes)
        // used to calculate error rate
        Map<String, Map<String, List<TestData>>> topicClientLoadMap = new ConcurrentHashMap<>();
        Map<String, Integer> topicThroughputQuotaMap = new HashMap<>();

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",", 3);
            String client = parts[0];
            String[] topicLoads = parts[2].split(":");
            String topic = parts[1];
            int throughputQuota = Integer.parseInt(topicLoads[0]);
            topicThroughputQuotaMap.put(topic, throughputQuota);
            when(topicLimitService.getThroughput(topic)).thenReturn(throughputQuota);

            // check if ratelimiterservice exists for a topic
            clientRateLimiterMap.putIfAbsent(client, createRateLimiterSvc());

            Runnable clientLoadSimulator = () -> {
                StringBuilder sb = new StringBuilder();
                sb.append("Client: ").append(client).append(" topic: ").append(topic).append(": \n");
//                long lastAllowedBytes = 0, lastRejectedBytes = 0;

                for (int i = 1; i < topicLoads.length; i++) {
                    String[] loadParts = topicLoads[i].split(",");
                    long duration = Long.parseLong(loadParts[0]) * 1000; // Convert to milliseconds
                    long throughput = Long.parseLong(loadParts[1]);
                    long qps = Long.parseLong(loadParts[2]);
//                        long allowedByClient = Long.parseLong(loadParts[3]);
                    long dataSize = throughput / qps;
                    long startTime = System.currentTimeMillis();
                    long allowedBytes = 0, rejectedBytes = 0;

                    RateLimiter rateLimiter = RateLimiter.create(qps);
                    while (System.currentTimeMillis() - startTime < duration) {
                        rateLimiter.acquire();
                        boolean allowed = clientRateLimiterMap.get(client).isAllowed(topic, dataSize);
                        if (allowed) {
                            allowedBytes += dataSize;
                        } else {
                            rejectedBytes += dataSize;
                        }
                    }
                    sb.append("Start Time: ").append(new Date(startTime))
                            .append(", Duration: ").append(duration)
                            .append(", throughput: ").append(throughput)
                            .append(", allowedBytes: ").append(allowedBytes)
                            .append(", rejectedBytes: ").append(rejectedBytes)
                            .append("\n");

                    // store result data for error rate calculation later on
                    log.info("[] Client: {}, Topic: {}, Duration: {}, Allowed Bytes: {}, Rejected Bytes: {}", client, topic, duration, allowedBytes, rejectedBytes);
                    storeResult(topicClientLoadMap, client, topic, duration/1000, allowedBytes, allowedBytes + rejectedBytes);
                }
                log.info(sb.toString());
            };

            threads.add(new Thread(clientLoadSimulator));
        }

        // Start all threads at the same time
        for (Thread thread : threads) {
            executorService.submit(thread);
        }

        // Wait for all threads to finish
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(100);
        }

        // Calculate error rate
        topicClientLoadMap.forEach((topic, clientLoadMap) -> {
            // here clientLoadMap is map of client -> List<TestData>
            // iterate through each client data assuming every client starts from the same point
            // consider duration and sum up the allowedBytes for each client and compare with the allowed throughput
            List<List<TestData>> clientDataList = new ArrayList<>();
            clientLoadMap.forEach((client, testDataList) -> {
                clientDataList.add(testDataList);
            });
            if(clientDataList.size() > 1) {
                List<Long> errors = calculateError(clientDataList, topicThroughputQuotaMap.get(topic));
                log.info("topic: {} errors: {}", topic, errors);
                log.info("topic: {} absolute error: {}", topic, calculateAbsoluteError(errors));
                log.info("Standard Deviation: {}", calculateStandardDeviation(errors));
            }
        });
    }

    private static List<Long> calculateError(List<List<TestData>> clientDataList, long throguhput) {
        List<TestData> result = new ArrayList<>();

        while (true) {
            long minDuration = Long.MAX_VALUE;
            long sumAllowedBytes = 0;
            long sumProducedBytes = 0;
            boolean hasMore = false;

            for (List<TestData> list : clientDataList) {
                if (!list.isEmpty()) {
                    hasMore = true;
                    TestData dv = list.get(0);
                    if (dv.duration < minDuration) {
                        minDuration = dv.duration;
                    }
                }
            }

            if (!hasMore) {
                break;
            }

            for (int i = 0, inputSize = clientDataList.size(); i < inputSize; i++) {
                List<TestData> list = clientDataList.get(i);
                if (!list.isEmpty()) {
                    TestData dv = list.get(0);
                    sumAllowedBytes += (minDuration * dv.allowedBytes / dv.duration);
                    sumProducedBytes += (minDuration * dv.generatedBytes / dv.duration);
                    if (dv.duration == minDuration) {
                        list.remove(0);
                    } else {
                        dv.allowedBytes -= (minDuration * dv.allowedBytes / dv.duration);
                        dv.generatedBytes -= (minDuration * dv.generatedBytes / dv.duration);
                        dv.duration -= minDuration;
                        if(dv.allowedBytes <= 0) {
                            throw new RuntimeException("Unexpected error");
                            // something failed here, find out why
                        }
                    }
                }
            }

            result.add(new TestData(minDuration, sumAllowedBytes, sumProducedBytes));
        }

        List<Long> errors = new ArrayList<>();
// 10KBPs 15KBps 7KBps +3
        for (TestData dv : result) {
            long maxBytes = Math.min(throguhput * dv.duration, dv.generatedBytes);
            long allowedBytes = dv.allowedBytes;
            errors.add(Math.abs(allowedBytes - maxBytes));
            System.out.println("Duration: " + dv.duration + ", Allowed Bytes: " + dv.allowedBytes + ", Generated Bytes: " + dv.generatedBytes);
        }

        return errors;
    }

    private static double calculateAbsoluteError(List<Long> errors) {
        return errors.stream().mapToLong(val -> val).sum();
    }

    private static double calculateStandardDeviation(List<Long> errors) {
        double mean = errors.stream().mapToDouble(val -> val).average().orElse(0.0);
        double variance = errors.stream().mapToDouble(val -> Math.pow(val - mean, 2)).average().orElse(0.0);
        return Math.sqrt(variance);
    }

    private void storeResult(
            Map<String, Map<String, List<TestData>>> topicClientLoadMap, String client, String topic, long duration,
            long allowedBytes, long generatedBytes
    ) {
        topicClientLoadMap.compute(topic, (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.compute(client, (k1, v1) -> {
                if (v1 == null) {
                    v1 = new ArrayList<>();
                }
                TestData testData = new TestData(duration, allowedBytes, generatedBytes);
                v1.add(testData);
                return v1;
            });
            return v;
        });
    }

    private RateLimiterService createRateLimiterSvc() throws UnknownHostException {
        return new RateLimiterService(
                new TrafficSender() {
                    @Override
                    public CompletableFuture<SuppressionData> send(ClientLoadInfo info) {
                        // receiver handler logic
                        SuppressionData suppressionData = new SuppressionData();
                        long delta = System.currentTimeMillis() - info.getTo();
                        log.info("Delta: {}ms", delta);
                        List<CompletableFuture<SuppressionFactor>> suppressionFactorFuture = new ArrayList<>();
                        info.getTopicUsageList().forEach((trafficData) -> {
                            suppressionFactorFuture.add(suppressionManager.addTrafficData(
                                    info.getClientId(),
                                    new TopicLoadInfo(info.getClientId(), info.getFrom(), info.getTo(), trafficData)
                            ).whenComplete((suppressionFactor, throwable) -> {
                                if (throwable != null) {
                                    log.error("Error while calculating suppression factor", throwable);
                                    return;
                                }
                                log.info(
                                        "Topic: {}, SF thr-pt: {}", trafficData.getTopic(),
                                        suppressionFactor.getThroughputFactor()
                                );
                                suppressionData.getSuppressionFactor().put(trafficData.getTopic(), suppressionFactor);
                            }));
                        });
                        return FutureUtil.waitForAll(suppressionFactorFuture).thenApply(__ -> suppressionData);
                    }
                },
                meterRegistry,
                1,
                HostUtils.getHostName()
        );
    }

    @Getter
    private static class TestData {
        private long duration;
        private long allowedBytes;
        private long generatedBytes;

        public TestData(long duration, long allowedBytes, long generatedBytes) {
            this.duration = duration;
            this.allowedBytes = allowedBytes;
            this.generatedBytes = generatedBytes;
        }
    }
}
