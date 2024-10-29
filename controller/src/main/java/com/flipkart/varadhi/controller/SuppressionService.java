package com.flipkart.varadhi.controller;

import com.flipkart.varadhi.qos.entity.ClientLoadInfo;
import com.flipkart.varadhi.qos.entity.SuppressionData;

import java.util.concurrent.CompletableFuture;

public interface SuppressionService {
    CompletableFuture<SuppressionData> addTrafficDataAsync(ClientLoadInfo info);

}
