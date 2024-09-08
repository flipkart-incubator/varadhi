package com.flipkart.varadhi.qos;


import com.flipkart.varadhi.entities.ratelimit.LoadInfo;
import com.flipkart.varadhi.entities.ratelimit.SuppressionData;
import com.flipkart.varadhi.entities.ratelimit.TrafficData;

import com.flipkart.varadhi.services.VaradhiTopicService;

import com.flipkart.varadhi.utils.weights.WeightFunction;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multiset;
import com.google.common.collect.PeekingIterator;

import com.google.common.collect.TreeMultiset;

import java.util.*;
import java.util.concurrent.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * - Initialise a priorityQueue (min heap) to maintain (time, order, data).
 * - Custom comparator to sort by time asc, order desc.
 *
 * Thread 1:
 * - pods will send data (from, to, data), break it into 2 events.
 * - (from, 1, +data/(to-from)) and (to, 0, -data/(to-from)) and add into PQ if
 * (from > (now() - Window size)) // skip adding if old data
 *
 * Thread 2: (pooling)
 * - maintain aggreagte value to use in check breach.
 * - peek and pop till now()
 * - compute running avg.
 * - check for RL. If true, add the suppression factor
 */

@Slf4j
public class RateLimiter {

    int windowSize;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final VaradhiTopicService varadhiTopicService;
    private final SuppressionData<Float> suppressionData;
    private final Multiset<LoadInfo> loadHistory;
    private WeightFunction weightFunction;
    private final List<RLObserver> observers = new ArrayList<>();
    private int qps_aggregated = 0;
    private int throughput_aggregated = 0;

    public RateLimiter(VaradhiTopicService varadhiTopicService, int windowSize, WeightFunction weightFunction) {

        this.varadhiTopicService = varadhiTopicService;
        this.windowSize = windowSize;
        this.weightFunction = weightFunction;

//        this.trafficHistoryMap = new ConcurrentHashMap<>();
        this.loadHistory = TreeMultiset.create();
//        this.suppressionFactorMap = new HashMap<>();
        this.suppressionData = new SuppressionData();

        sendSuppressionFactorToWeb();
    }

    public void addTrafficData(LoadInfo dump) {
        loadHistory.add(dump);
        log.info("received data: " + dump.getTopicUsageMap());
        log.info("current loadHistory: " + loadHistory);
//        dump.getTopicUsageMap().forEach((topic, data) -> {
//            addTrafficData(topic, dump.getFrom(), dump.getTo(), data);
//        });
    }

    public void sendSuppressionFactorToWeb() {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            log.info("Update suppression factor");
            var trafficHistoryMap = generateDS();
            log.info("generated trafficHistoryMap: " + trafficHistoryMap);
            sendSuppressionFactor(trafficHistoryMap);
        }, 0, windowSize, TimeUnit.SECONDS);
    }

    private Map<String, Multiset<Pair<Long, TrafficData>>> generateDS() {
        // takes loadHistory and generate trafficHistoryMap;
        Map<String, Multiset<Pair<Long, TrafficData>>> trafficHistoryMap = new HashMap<>();
        // remove old values
        log.info("before remove: " + loadHistory);
        long popTill = System.currentTimeMillis() - (windowSize * 1000L);
        log.info("popTill: " + popTill);
        Iterator<LoadInfo> iterator = loadHistory.iterator();
        while (iterator.hasNext()) {
            LoadInfo loadInfo = iterator.next();
            if (loadInfo.getTo() < popTill) {
                log.info("removing: " + loadInfo);
                iterator.remove();
            }
        }
//        loadHistory.forEach(loadInfo -> {
//            if(loadInfo.getTo() < popTill) {
//                loadHistory.remove(loadInfo);
//            }
//        });
//        loadHistory.removeIf(loadInfo -> {
//            return loadInfo.getTo() < popTill;
//        });
        log.info("after remove: " + loadHistory);
        loadHistory.forEach(loadInfo -> loadInfo.getTopicUsageMap().forEach((topic, data) -> {
            trafficHistoryMap.computeIfAbsent(topic, k -> TreeMultiset.create());
            process(trafficHistoryMap, topic, loadInfo.getFrom(), loadInfo.getTo(), data);
        }));
        return trafficHistoryMap;
    }

    private void process(Map<String, Multiset<Pair<Long, TrafficData>>> trafficHistoryMap, String topic, long from, long to, TrafficData data) {
        if (to - from > windowSize * 1000L) {
            log.warn("Data is too big to be added to the rate limiter, decrease the frequency of producers or increase the window size");
            return;
        }
        if (to < (System.currentTimeMillis() - windowSize * 1000L)) {
            log.warn("to: " + to + " and now: " + (System.currentTimeMillis() - windowSize * 1000L));
            log.warn("Data is too old to be added to the rate limiter");
            return;
        }
        // add data to the map
        trafficHistoryMap.computeIfAbsent(topic, k -> TreeMultiset.create());
        trafficHistoryMap.get(topic).add(Pair.of(from, data));
        trafficHistoryMap.get(topic).add(Pair.of(
                to, TrafficData.builder().rateIn(-data.getRateIn()).throughputIn(-data.getThroughputIn()).build()));
    }

    private void sendSuppressionFactor(Map<String, Multiset<Pair<Long, TrafficData>>> trafficHistoryMap) {
        trafficHistoryMap.forEach((topic, data) -> {
            int qps = varadhiTopicService.get(topic).getCapacity().getQps();
            int throughput_bps = varadhiTopicService.get(topic).getCapacity().getThroughputKBps() * 1024;
            float sf = computeSuppressionFactor(data, qps, throughput_bps);
            suppressionData.getSuppressionFactor().put(topic, sf);
        });
        observers.forEach(this::update);
    }

    /**
     *  (pooling)
     *  maintain aggreagte value to use in check breach.
     *  peek and pop till now()
     *  compute running avg.
     *  check for RL. If true, add the suppression factor
     */
    private float computeSuppressionFactor(Multiset<Pair<Long, TrafficData>> data, int qps, int throughput_bps) {

        long NOW = System.currentTimeMillis();

        PeekingIterator<Pair<Long, TrafficData>> it = Iterators.peekingIterator(data.iterator());
        Map<Long, Float> weightedSuppressionFactorMap = new HashMap<>();
        while(it.hasNext()) {
            Pair<Long, TrafficData> pair = it.next();
            Long time = pair.getLeft();
            TrafficData trafficData = pair.getRight();

            log.info("time: " + new Date(time));

            qps_aggregated += trafficData.getRateIn();
            throughput_aggregated += trafficData.getThroughputIn();

            log.info("## qps: " + trafficData.getRateIn() + " throughput_bps: " + trafficData.getThroughputIn());
            log.info("## qps_aggregated: " + qps_aggregated + " throughput_aggregated: " + throughput_aggregated);

            // compute aggregated values (at same time)
            while(it.hasNext() && Objects.equals(time, it.peek().getLeft())) {
                log.info("## same ## qps_aggregated: " + qps_aggregated + " throughput_aggregated: " + throughput_aggregated);
                pair = it.next();
                trafficData = pair.getRight();
                qps_aggregated += trafficData.getRateIn();
                throughput_aggregated += trafficData.getThroughputIn();
            }

            // TODO(rl): divide by time difference
            float sf_qps = Math.max(0f, (1.0f - ((float) qps / qps_aggregated)));
            float sf_throughput = Math.max(0f, (1.0f - ((float) throughput_bps / throughput_aggregated)));
            log.info("--------------------");
            log.info("time: " + new Date(time));
            log.info("qps: " + qps + " throughput: " + throughput_bps);
            log.info("qps_aggregated: " + qps_aggregated + " throughput_aggregated: " + throughput_aggregated);
            log.info("sf_qps: " + sf_qps + " sf_throughput: " + sf_throughput);
            log.info("--------------------");
            weightedSuppressionFactorMap.put(time, Math.max(sf_qps, sf_throughput));
        }

        // apply weights on suppression factor map and return the value
        float totalWeight = 0;
        float weightedSum = 0;
        for (Map.Entry<Long, Float> entry : weightedSuppressionFactorMap.entrySet()) {
            if(entry.getValue() <= 0) {
                continue;
            }
            float weight = weightFunction.applyWeight(entry.getKey(), NOW, windowSize * 1000L);
            totalWeight += weight;
            weightedSum += weight * entry.getValue();
        }

        log.info("totalWeight: " + totalWeight + " weightedSum: " + weightedSum);
        log.info("suppression factor: " + (totalWeight == 0 ? 0 : weightedSum / totalWeight));
        // handle case if no datapoints
        return totalWeight == 0 ? 0 : weightedSum / totalWeight;
    }

    public void registerObserver(RLObserver observer) {
        observers.add(observer);
    }

    public void update(RLObserver observer) {
        observer.update(suppressionData);
    }
}
