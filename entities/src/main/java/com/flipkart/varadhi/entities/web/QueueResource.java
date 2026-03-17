package com.flipkart.varadhi.entities.web;

import com.flipkart.varadhi.entities.CallbackConfig;
import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.Validatable;
import com.flipkart.varadhi.entities.web.request.SubscriptionRequestModel;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * Queue resource - base + queue-specific fields (topic + subscription aspects for a queue).
 */
@Getter
@Setter
public class QueueResource extends BaseResource implements Validatable {
    private static final String QUEUE_SUBSCRIPTION_SUFFIX = "-queue";

    // Queue extra fields (for topic part)
    private Object storageTopicDetails;
    private Boolean mirror;
    private String activeProduceZone;
    private List<?> storageTopics;
    private TopicCapacityPolicy capacity;
    private LifecycleStatus.ActionCode actionCode;
    private String nfrFilterName;

    // Queue extra fields (for subscription part)
    private Integer noOfConsumers;
    private RetryPolicy retryPolicy;
    private CallbackConfig callbackConfig;
    private Map<String, List<String>> targetClientIds;

    /**
     * Default subscription name for a queue (topic name + suffix).
     */
    public static String getDefaultSubscriptionName(String queueOrTopicName) {
        return queueOrTopicName + QUEUE_SUBSCRIPTION_SUFFIX;
    }

    /**
     * Builds TopicResource from this queue resource (topic part) for the given project and action code.
     */
    public TopicResource toTopicResource(String projectFromPath, LifecycleStatus.ActionCode actionCode) {
        String projectName = this.project != null && !this.project.isBlank() ? this.project : projectFromPath;
        TopicCapacityPolicy cap = this.capacity != null ? this.capacity : TopicCapacityPolicy.getDefault();
        LifecycleStatus.ActionCode code = actionCode != null ? actionCode : LifecycleStatus.ActionCode.USER_ACTION;
        String nfr = this.nfrFilterName != null ? this.nfrFilterName : (this.getNfrStrategy() != null ? this.getNfrStrategy() : "");

        if (Boolean.TRUE.equals(this.getGrouped())) {
            return TopicResource.grouped(this.getName(), projectName, cap, code, nfr);
        }
        return TopicResource.unGrouped(this.getName(), projectName, cap, code, nfr);
    }

    /**
     * Builds SubscriptionRequestModel from this queue resource (subscription part).
     */
    public SubscriptionRequestModel toSubscriptionRequest() {
        SubscriptionRequestModel sub = new SubscriptionRequestModel();
        copyBaseTo(sub);
        sub.setName(getDefaultSubscriptionName(this.getName()));
        sub.setTopicName(this.getName());
        sub.setSubscriptionMode("QUEUE_LIKE");
        sub.setNoOfConsumers(this.getNoOfConsumers());
        sub.setRetryPolicy(this.getRetryPolicy());
        sub.setCallbackConfig(this.getCallbackConfig());
        sub.setTargetClientIds(this.getTargetClientIds());
        return sub;
    }

    private void copyBaseTo(BaseResource target) {
        target.setName(this.getName());
        target.setProject(this.getProject());
        target.setTeam(this.getTeam());
        target.setSecured(this.getSecured());
        target.setGrouped(this.getGrouped());
        target.setAppId(this.getAppId());
        target.setNfrStrategy(this.getNfrStrategy());
    }
}
