package com.flipkart.varadhi.server.spi.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.regex.Pattern;

@Getter
@NoArgsConstructor
public class URLDefinition {
    private String path;
    private List<String> methodList;

    @JsonIgnore
    private Pattern urlPattern;

    public URLDefinition(String path, List<String> methodList) {
        this.methodList = methodList;
        this.setPath(path);
    }

    private void compilePattern() {
        this.urlPattern = Pattern.compile(path);
    }

    public void setPath(String path) {
        this.path = path;
        compilePattern();
    }

    public void setMethodList(List<String> methodList) {
        this.methodList = methodList;
    }
}
