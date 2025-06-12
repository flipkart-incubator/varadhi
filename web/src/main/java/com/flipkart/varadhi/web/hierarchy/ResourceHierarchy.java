package com.flipkart.varadhi.web.hierarchy;


import java.util.Map;

public interface ResourceHierarchy {
    String getResourcePath();

    Map<String, String> getAttributes();
}
