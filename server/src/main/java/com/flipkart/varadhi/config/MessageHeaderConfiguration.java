package com.flipkart.varadhi.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.util.List;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Valid
public class MessageHeaderConfiguration {
    @NotNull
    private List<String> allowedPrefix;

    // Callback codes header
    @NotNull
    private String callbackCodes;

    // Consumer timeout header (indefinite message consumer timeout)
    @NotNull
    private String requestTimeout;

    // HTTP related headers
    @NotNull
    private String replyToHttpUriHeader;

    @NotNull
    private String replyToHttpMethodHeader;

    @NotNull
    private String replyToHeader;

    @NotNull
    private String httpUriHeader;

    @NotNull
    private String httpMethodHeader;

    @NotNull
    private String httpContentType;

    // Group ID & Msg ID header used to correlate messages
    @NotNull
    private String groupIdHeader;

    @NotNull
    private String msgIdHeader;
    public static boolean validateHeaderMapping(MessageHeaderConfiguration messageHeaderConfiguration)
            throws IllegalAccessException {
        for (Field field : MessageHeaderConfiguration.class.getDeclaredFields()) {
            if (field.getName().equals("allowedPrefix")) {
                continue;
            }
            Object value = field.get(messageHeaderConfiguration);
            if (!startsWithValidPrefix(messageHeaderConfiguration.getAllowedPrefix(), (String) value)) {
                throw new IllegalArgumentException(
                        String.format("Header '%s' with value '%s' doesn't start with any valid prefix",
                                field.getName(), value
                        ));
            }
        }
        return true;
    }

    private static boolean startsWithValidPrefix(List<String> prefixList, String value) {
        for (String prefix : prefixList) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

}
