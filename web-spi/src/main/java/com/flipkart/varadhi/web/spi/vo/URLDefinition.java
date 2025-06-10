package com.flipkart.varadhi.web.spi.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.regex.Pattern;

@Getter
@NoArgsConstructor
public class URLDefinition {

    @NotBlank
    @NonNull
    private String path;

    @NotNull
    @NotEmpty
    private List<String> methodList;

    @JsonIgnore
    private Pattern urlPattern;

    public static boolean anyMatch(List<URLDefinition> urlDefinitions, String method, String path) {
        for (URLDefinition urlDefinition : urlDefinitions) {
            if (urlDefinition.getMethodList().contains(method) && urlDefinition.getUrlPattern()
                                                                               .matcher(path)
                                                                               .matches()) {
                return true;
            }
        }
        return false;
    }

    public URLDefinition(String path, List<String> methodList) {
        this.methodList = methodList;
        this.setPath(path);
    }

    private void compilePattern() {
        try {
            this.urlPattern = Pattern.compile(path);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid regex pattern: " + path, e);
        }
    }

    public void setPath(String path) {
        this.path = path;
        compilePattern();
    }

    public void setMethodList(List<String> methodList) {
        this.methodList = methodList;
    }
}
