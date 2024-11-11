package com.flipkart.varadhi.qos.entity;

import java.util.List;

/**
 * The LoadPredictor interface provides a method to predict the topic load coming from different clients.
 * Implementations of this interface should provide the logic to predict the load. (Eg based on historical data, most
 * recent data, etc.)
 */
public interface LoadPredictor {

    /**
     * Add the load information of a topic for a given client;
     * @param clientId unique clientId
     * @param load topic load information
     */
    void add(String clientId, TopicLoadInfo load);

    /**
     * Predicts the load of topic from different clients.
     *
     * @return a list of TopicLoadInfo objects representing the predicted load of topic from different clients.
     */
    List<TopicLoadInfo> predictLoad();
}
