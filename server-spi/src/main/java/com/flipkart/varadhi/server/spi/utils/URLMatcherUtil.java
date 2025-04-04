package com.flipkart.varadhi.server.spi.utils;

import com.flipkart.varadhi.server.spi.vo.URLDefinition;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class URLMatcherUtil {
    private List<URLDefinition> urlDefinitionList;

    public boolean matches(String method, String path) {
        for (URLDefinition urlDefinition : urlDefinitionList) {
            if (urlDefinition.getMethodList().contains(method) && urlDefinition.getUrlPattern()
                                                                               .matcher(path)
                                                                               .matches()) {
                return true;
            }
        }
        return false;
    }
}
