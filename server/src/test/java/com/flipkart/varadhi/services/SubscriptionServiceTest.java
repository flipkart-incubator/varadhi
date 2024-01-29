package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.MalformedURLException;
import java.util.List;

import static com.flipkart.varadhi.web.admin.SubscriptionHandlersTest.getVaradhiSubscription;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class SubscriptionServiceTest {

    private final Project project = new Project("project1", 0, "", "team1", "org1");
    private final VaradhiTopic unGroupedTopic = VaradhiTopic.of(new TopicResource("topic1", 0, "project2", false, null));
    private final VaradhiTopic groupedTopic = VaradhiTopic.of(new TopicResource("topic2", 0, "project2", true, null));

    MetaStore metaStore;
    SubscriptionService subscriptionService;

    @BeforeEach
    void setUp() {
        metaStore = mock(MetaStore.class);
        subscriptionService = new SubscriptionService(metaStore);
    }

    @Test
    void getSubscriptionListReturnsCorrectSubscriptions() {
        String projectName = "testProject";
        List<String> expectedSubscriptions = List.of("sub1", "sub2", "sub3");

        when(metaStore.getSubscriptionNames(projectName)).thenReturn(expectedSubscriptions);

        List<String> actualSubscriptions = subscriptionService.getSubscriptionList(projectName);

        assertEquals(expectedSubscriptions, actualSubscriptions);
    }

    @Test
    void getSubscriptionReturnsCorrectSubscription() throws MalformedURLException {
        String subscriptionName = "sub1";
        VaradhiSubscription expectedSubscription = getVaradhiSubscription(subscriptionName, project, unGroupedTopic);

        when(metaStore.getSubscription(subscriptionName)).thenReturn(expectedSubscription);

        VaradhiSubscription actualSubscription = subscriptionService.getSubscription(subscriptionName);

        assertEquals(expectedSubscription, actualSubscription);
    }

    @Test
    void getSubscriptionForNonExistentThrows() {
        String subscriptionName = "sub1";

        when(metaStore.getSubscription(subscriptionName)).thenThrow(new ResourceNotFoundException("Subscription not found"));

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            subscriptionService.getSubscription(subscriptionName);
        });

        String expectedMessage = "Subscription not found";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscription() throws MalformedURLException {
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", project, unGroupedTopic);

        var projectNameCaptor = ArgumentCaptor.forClass(String.class);
        var topicNameCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(metaStore.getProject(projectNameCaptor.capture())).thenReturn(project);
        Mockito.when(metaStore.getTopic(topicNameCaptor.capture())).thenReturn(unGroupedTopic);

        VaradhiSubscription result = subscriptionService.createSubscription(subscription);

        assertEquals(subscription, result);
        assertEquals(project.getName(), projectNameCaptor.getValue());
        assertEquals(unGroupedTopic.getName(), topicNameCaptor.getValue());
        Mockito.verify(metaStore).createSubscription(subscription);
    }

    @Test
    void testCreateSubscriptionWithNonGroupedTopic() throws MalformedURLException {
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", true, project, unGroupedTopic);

        Mockito.when(metaStore.getProject(anyString())).thenReturn(project);
        Mockito.when(metaStore.getTopic(anyString())).thenReturn(unGroupedTopic);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            subscriptionService.createSubscription(subscription);
        });

        String expectedMessage = "Cannot create grouped Subscription as it's Topic(project2.topic1) is not grouped";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscriptionWithGroupedTopic() throws MalformedURLException {
        VaradhiSubscription unGroupedSub = getVaradhiSubscription("sub1", false, project, groupedTopic);
        VaradhiSubscription groupedSub = getVaradhiSubscription("sub1", true, project, groupedTopic);

        Mockito.when(metaStore.getProject(anyString())).thenReturn(project);
        Mockito.when(metaStore.getTopic(anyString())).thenReturn(groupedTopic);

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(unGroupedSub);
        });

        assertDoesNotThrow(() -> {
            subscriptionService.createSubscription(groupedSub);
        });
    }

    @Test
    void testCreateSubscriptionWithNonExistentProject() throws MalformedURLException {
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", project, unGroupedTopic);

        Mockito.when(metaStore.getProject(anyString())).thenThrow(new ResourceNotFoundException("Project not found"));

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            subscriptionService.createSubscription(subscription);
        });

        String expectedMessage = "Project not found";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void testCreateSubscriptionWithNonExistentTopic() throws MalformedURLException {
        VaradhiSubscription subscription = getVaradhiSubscription("sub1", project, unGroupedTopic);

        Mockito.when(metaStore.getProject(anyString())).thenReturn(project);
        Mockito.when(metaStore.getTopic(anyString())).thenThrow(new ResourceNotFoundException("Topic not found"));

        Exception exception = assertThrows(ResourceNotFoundException.class, () -> {
            subscriptionService.createSubscription(subscription);
        });

        String expectedMessage = "Topic not found";
        String actualMessage = exception.getMessage();

        assertEquals(expectedMessage, actualMessage);
    }

    @Test
    void updateSubscriptionUpdatesCorrectly() throws MalformedURLException {
        VaradhiSubscription existingSubscription = getVaradhiSubscription("sub1", project, unGroupedTopic);

        VaradhiSubscription newSubscription = getVaradhiSubscription("sub1", project, unGroupedTopic);
        newSubscription.setVersion(1);

        Mockito.when(metaStore.getSubscription(anyString())).thenReturn(existingSubscription);
        Mockito.when(metaStore.updateSubscription(any())).thenReturn(2);

        VaradhiSubscription updatedSubscription = subscriptionService.updateSubscription(newSubscription);

        assertEquals(newSubscription.getDescription(), updatedSubscription.getDescription());
        assertEquals(2, updatedSubscription.getVersion());
    }

    @Test
    void updateSubscriptionTopicNotAllowed() throws MalformedURLException {
        VaradhiSubscription existingSubscription = getVaradhiSubscription("sub1", project, unGroupedTopic);

        VaradhiSubscription newSubscription = getVaradhiSubscription("sub1", project, groupedTopic);
        newSubscription.setVersion(1);

        Mockito.when(metaStore.getSubscription(anyString())).thenReturn(existingSubscription);

        Exception ex = assertThrows(java.lang.IllegalArgumentException.class, () -> {
            subscriptionService.updateSubscription(newSubscription);
        });

        assertEquals("Cannot update Topic of Subscription(project1.sub1)", ex.getMessage());
    }

    @Test
    void updateSubscriptionWithVersionConflictThrows() throws MalformedURLException {
        VaradhiSubscription existingSubscription = getVaradhiSubscription("sub1", project, unGroupedTopic);
        existingSubscription.setVersion(1);

        VaradhiSubscription newSubscription = getVaradhiSubscription("sub1", project, unGroupedTopic);
        newSubscription.setVersion(2);

        Mockito.when(metaStore.getSubscription(anyString())).thenReturn(existingSubscription);

        assertThrows(InvalidOperationForResourceException.class, () -> {
            subscriptionService.updateSubscription(newSubscription);
        });
    }

    @Test
    void updateSubscriptionWithUnGroupedTopicThrows() throws MalformedURLException {
        VaradhiSubscription existingSubscription = getVaradhiSubscription("sub1", false, project, unGroupedTopic);

        VaradhiSubscription newSubscription = getVaradhiSubscription("sub1", true, project, unGroupedTopic);

        Mockito.when(metaStore.getSubscription(anyString())).thenReturn(existingSubscription);
        Mockito.when(metaStore.getTopic(anyString())).thenReturn(unGroupedTopic);

        assertThrows(IllegalArgumentException.class, () -> {
            subscriptionService.updateSubscription(newSubscription);
        });
    }

    @Test
    void deleteSubscriptionRemovesSubscription() {
        String subscriptionName = "sub1";

        subscriptionService.deleteSubscription(subscriptionName);

        verify(metaStore, times(1)).deleteSubscription(subscriptionName);
    }

}
