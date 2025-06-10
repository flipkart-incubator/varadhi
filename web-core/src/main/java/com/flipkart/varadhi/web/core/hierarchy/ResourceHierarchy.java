package com.flipkart.varadhi.web.core.hierarchy;


import java.util.Map;

public interface ResourceHierarchy {
    String getResourcePath();

    Map<String, String> getAttributes();
}
