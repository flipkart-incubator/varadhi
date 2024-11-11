package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;

/**
 * DistributedRateLimiter interface that takes in loadInfo from all the clients and returns the SuppressionData for each
 * topic so that FactorRateLimiter can use it to limit the traffic.
 */
public interface DistributedRateLimiter extends RateLimiter<ClientLoadInfo, SuppressionData> {

}
