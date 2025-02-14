package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.Constants;
import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.StorageSubscription;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.spi.services.StorageSubscriptionFactory;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import com.flipkart.varadhi.web.admin.SubscriptionTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class VaradhiSubscriptionFactoryTest extends SubscriptionTestBase {

    private static final String REGION = "local";
    private static final String SUBSCRIPTION_NAME = "testSubscription";
    private static final TopicCapacityPolicy CAPACITY_POLICY = Constants.DEFAULT_TOPIC_CAPACITY;

    @Mock
    private StorageSubscriptionFactory<StorageSubscription<StorageTopic>, StorageTopic> subscriptionFactory;

    @Mock
    private StorageTopicFactory<StorageTopic> topicFactory;

    @Mock
    private StorageTopicService<StorageTopic> topicService;

    @InjectMocks
    private VaradhiSubscriptionFactory varadhiSubscriptionFactory;

    @Override
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        varadhiSubscriptionFactory = new VaradhiSubscriptionFactory(
            topicService,
            subscriptionFactory,
            topicFactory,
            REGION
        );
    }

    @Test
    void getShardCapacity_ValidInput_ReturnsCorrectCapacity() throws Exception {
        Method method = VaradhiSubscriptionFactory.class.getDeclaredMethod(
            "getShardCapacity",
            TopicCapacityPolicy.class,
            int.class
        );
        method.setAccessible(true);

        TopicCapacityPolicy result = (TopicCapacityPolicy)method.invoke(varadhiSubscriptionFactory, CAPACITY_POLICY, 2);

        assertNotNull(result);
        assertEquals(CAPACITY_POLICY.getReadFanOut(), result.getReadFanOut());
    }

    @Test
    void getShardMainSubName_ValidInput_ReturnsCorrectName() throws Exception {
        Method method = VaradhiSubscriptionFactory.class.getDeclaredMethod(
            "getShardMainSubName",
            String.class,
            int.class
        );
        method.setAccessible(true);

        String result = (String)method.invoke(varadhiSubscriptionFactory, SUBSCRIPTION_NAME, 0);

        assertEquals("testSubscription.shard.0.MAIN", result);
    }

    @Test
    void getInternalSubName_ValidInput_ReturnsCorrectName() throws Exception {
        Method method = VaradhiSubscriptionFactory.class.getDeclaredMethod(
            "getInternalSubName",
            String.class,
            int.class,
            InternalQueueCategory.class,
            int.class
        );
        method.setAccessible(true);

        String result = (String)method.invoke(
            varadhiSubscriptionFactory,
            SUBSCRIPTION_NAME,
            0,
            InternalQueueCategory.MAIN,
            0
        );

        assertEquals("is-testSubscription.shard-0.MAIN-0", result);
    }

    @Test
    void getInternalTopicName_ValidInput_ReturnsCorrectName() throws Exception {
        Method method = VaradhiSubscriptionFactory.class.getDeclaredMethod(
            "getInternalTopicName",
            String.class,
            int.class,
            InternalQueueCategory.class,
            int.class
        );
        method.setAccessible(true);

        String result = (String)method.invoke(
            varadhiSubscriptionFactory,
            SUBSCRIPTION_NAME,
            0,
            InternalQueueCategory.MAIN,
            0
        );

        assertEquals("it-testSubscription.shard-0.MAIN-0", result);
    }
}
