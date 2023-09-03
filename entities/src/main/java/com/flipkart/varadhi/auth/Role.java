package com.flipkart.varadhi.auth;

import java.util.List;

public record Role(String id, List<ResourceAction> permissions){}
