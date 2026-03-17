package com.flipkart.varadhi.entities.web.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Key-value pair for subscription filter configuration.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FilterKeyValuePair {
    private String key;
    private String value;
}
