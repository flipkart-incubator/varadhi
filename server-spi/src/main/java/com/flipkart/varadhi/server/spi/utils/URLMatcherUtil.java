package com.flipkart.varadhi.server.spi.utils;

import com.flipkart.varadhi.server.spi.vo.URLDefinition;

import java.util.List;
import java.util.regex.Pattern;

public class URLMatcherUtil {
    private List<URLDefinition> urlDefinitionList;

    public URLMatcherUtil(List<URLDefinition> urlDefinitionList) {
        for (URLDefinition urlDefinition : urlDefinitionList) {
            urlDefinition.setUrlPattern(Pattern.compile(urlDefinition.getPath()));
        }
        this.urlDefinitionList = urlDefinitionList;
    }

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
