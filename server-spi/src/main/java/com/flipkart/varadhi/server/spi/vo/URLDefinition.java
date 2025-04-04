package com.flipkart.varadhi.server.spi.vo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.regex.Pattern;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class URLDefinition {
    private String path;
    private List<String> methodList;

    @JsonIgnore
    private Pattern urlPattern;
}
