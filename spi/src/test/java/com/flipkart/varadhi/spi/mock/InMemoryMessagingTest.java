package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.SimpleMessage;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.TestStdHeaders;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.spi.services.Producer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class InMemoryMessagingTest {

    @Test
    public void testSimpleProduce() throws ExecutionException, InterruptedException {
        StdHeaders.init(TestStdHeaders.get());
        InMemoryMessagingStackProvider provider = new InMemoryMessagingStackProvider();
        provider.init(null, null);

        Project project = Project.of("testProject", "testDescription", "testTeam", "testOrg");
        TopicCapacityPolicy policy = new TopicCapacityPolicy(100, 1000, 1);
        InMemoryStorageTopic topic = provider.getStorageTopicFactory()
                                             .getTopic(1, "testTopic", project, policy, InternalQueueCategory.MAIN);

        Assertions.assertEquals(2, topic.getPartitions());

        provider.getStorageTopicService().create(topic, project);

        Producer producer = provider.getProducerFactory().newProducer(topic, policy);

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StdHeaders.get().msgId(), "msgId-1");
        Message msg = new SimpleMessage(new byte[] {}, headers);

        Offset[] offsets = new Offset[10];
        for (int i = 0; i < 10; i++) {
            offsets[i] = producer.produceAsync(msg).get();
        }

        List<InMemoryOffset> inMemoryOffsets = Arrays.stream(offsets).map(o -> (InMemoryOffset)o).toList();
        Map<Integer, List<Long>> observed = new HashMap<>();
        observed.put(0, new ArrayList<>());
        observed.put(1, new ArrayList<>());

        inMemoryOffsets.forEach(o -> {
            int partition = o.partition();
            long offset = o.offset();
            Assertions.assertTrue(partition == 0 || partition == 1);
            Assertions.assertTrue(offset >= 0 && offset < 10);
            observed.get(partition).add(offset);
        });

        // check that offsets are already sorted
        observed.forEach((partition, offsetsList) -> {
            for (int i = 0; i < offsetsList.size() - 1; i++) {
                Assertions.assertTrue(
                    offsetsList.get(i) < offsetsList.get(i + 1),
                    "Offsets in partition " + partition + " are not sorted: " + offsetsList
                );
            }
        });
    }
}
