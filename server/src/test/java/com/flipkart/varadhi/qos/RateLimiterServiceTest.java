package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.CoreServices;
import com.flipkart.varadhi.config.AppConfiguration;
import com.flipkart.varadhi.controller.DistributedRateLimiterImpl;
import com.flipkart.varadhi.core.capacity.TopicCapacityService;
import com.flipkart.varadhi.utils.HostUtils;
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
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.flipkart.varadhi.VaradhiApplication.readConfiguration;
import static org.mockito.Mockito.when;

@Slf4j
public class RateLimiterServiceTest {

    private static MeterRegistry meterRegistry;
    private DistributedRateLimiterImpl distributedRateLimiterImpl;
    @Mock
    private TopicCapacityService topicCapacityService;

    @BeforeAll
    public static void setup() throws UnknownHostException {
        HostUtils.initHostUtils();
        setupMetrics();
    }

    public static void setupMetrics() {
        String[] args = {"src/test/resources/testConfiguration.yml"};
        AppConfiguration configuration = readConfiguration(args).getKey();
        CoreServices.ObservabilityStack observabilityStack = new CoreServices.ObservabilityStack(configuration);
        meterRegistry = observabilityStack.getMeterRegistry();
    }

    @BeforeEach
    public void setUpController() {
        MockitoAnnotations.openMocks(this); // Initialize mocks

        //setup controller side of things
        Clock clock = Clock.systemUTC();
        distributedRateLimiterImpl = new DistributedRateLimiterImpl(10, 1000, topicCapacityService, clock);
    }

    private static Stream<Arguments> provideRateLimitTestFilePaths() {
        return Stream.of(
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_low.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_low.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_medium.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_medium.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_high.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_high.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_very_high.profile"),
                Arguments.of("src/test/resources/simulation_profiles/test_skewness_very_high.profile")
        );
    }

    @ParameterizedTest
    @MethodSource("provideRateLimitTestFilePaths")
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
            log.info("Setting throughput for topic: {}, throughput: {}", topic, throughputQuota);
            when(topicCapacityService.getThroughputLimit(topic)).thenReturn(throughputQuota);

            // check if ratelimiterservice exists for a topic
            clientRateLimiterMap.putIfAbsent(client, createRateLimiterSvc(client));

            Runnable clientLoadSimulator = () -> {
                StringBuilder sb = new StringBuilder();
                sb.append("Client: ").append(client).append(" topic: ").append(topic).append(": \n");

                for (int i = 1; i < topicLoads.length; i++) {
                    String[] loadParts = topicLoads[i].split(",");
                    long duration = Long.parseLong(loadParts[0]) * 1000; // Convert to milliseconds
                    long throughput = Long.parseLong(loadParts[1]);
                    long qps = Long.parseLong(loadParts[2]);
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
                    log.info(
                            "[] Client: {}, Topic: {}, Duration: {}, Allowed Bytes: {}, Rejected Bytes: {}", client,
                            topic, duration, allowedBytes, rejectedBytes
                    );
                    storeResult(
                            topicClientLoadMap, client, topic, duration / 1000, allowedBytes,
                            allowedBytes + rejectedBytes
                    );
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
            clientLoadMap.forEach((client, testDataList) -> clientDataList.add(testDataList));
            if (clientDataList.size() > 1) {
                List<Double> errors = calculateNormalisedError(clientDataList, topicThroughputQuotaMap.get(topic));
                log.info("topic: {} errors: {}", topic, errors);
                log.info("topic: {} absolute error: {}", topic, calculateAbsoluteError(errors));
                log.info("topic: {} mean error: {}", topic, calculateAbsoluteError(errors)/errors.size());
                log.info("topic: {} standard deviation: {}", topic, calculateStandardDeviation(errors));
            }
        });
    }

    private static List<Double> calculateNormalisedError(List<List<TestData>> clientDataList, long throguhput) {
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

            for (List<TestData> clientTestData : clientDataList) {
                if (!clientTestData.isEmpty()) {
                    TestData dv = clientTestData.get(0);
                    sumAllowedBytes += (minDuration * dv.allowedBytes / dv.duration);
                    sumProducedBytes += (minDuration * dv.generatedBytes / dv.duration);
                    if (dv.duration == minDuration) {
                        clientTestData.remove(0);
                    } else {
                        dv.allowedBytes -= (minDuration * dv.allowedBytes / dv.duration);
                        dv.generatedBytes -= (minDuration * dv.generatedBytes / dv.duration);
                        dv.duration -= minDuration;
                        if (dv.allowedBytes <= 0) {
                            throw new RuntimeException("Unexpected error");
                            // something failed here, find out why
                        }
                    }
                }
            }

            result.add(new TestData(minDuration, sumAllowedBytes, sumProducedBytes));
        }

        List<Double> errors = new ArrayList<>();
        for (TestData dv : result) {
            long maxBytes = Math.min(throguhput * dv.duration, dv.generatedBytes);
            long allowedBytes = dv.allowedBytes;
            errors.add(((double) (Math.abs(allowedBytes - maxBytes)) / throguhput));
            log.info(
                    "Duration: {}, Allowed Bytes: {}, Generated Bytes: {}", dv.duration, dv.allowedBytes,
                    dv.generatedBytes
            );
        }

        return errors;
    }

    private static double calculateAbsoluteError(List<Double> errors) {
        return errors.stream().mapToDouble(val -> val).sum();
    }

    private static double calculateStandardDeviation(List<Double> errors) {
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

    private RateLimiterService createRateLimiterSvc(String clientId) throws UnknownHostException {
        return new RateLimiterService(
                load -> distributedRateLimiterImpl.addTrafficData(load),
                new RateLimiterMetrics(meterRegistry, clientId), 1, clientId
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
