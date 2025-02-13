package com.flipkart.varadhi.config;

import com.flipkart.varadhi.entities.Validatable;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.lang.reflect.Method;
import java.util.List;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MessageHeaderConfiguration implements Validatable {
    @NotNull private List<String> allowedPrefix;

    // Callback codes header
    @NotNull private String callbackCodes;

    // Consumer timeout header (indefinite message consumer timeout)
    @NotNull private String requestTimeout;

    // HTTP related headers
    @NotNull private String replyToHttpUriHeader;

    @NotNull private String replyToHttpMethodHeader;

    @NotNull private String replyToHeader;

    @NotNull private String httpUriHeader;

    @NotNull private String httpMethodHeader;

    @NotNull private String httpContentType;

    // Group ID & Msg ID header used to correlate messages
    @NotNull private String groupIdHeader;

    @NotNull private String msgIdHeader;

    /**
     * We use reflection to dynamically invoke getter methods for all fields in the
     * `MessageHeaderConfiguration` class, allowing us to validate them without
     * explicitly coding checks for each field. This makes the validation process
     * scalable and easier to maintain, as any new fields with getters will be
     * automatically validated. The `allowedPrefix` field is skipped as it is handled separately.
     */
    @SneakyThrows
    @Override
    public void validate() {
        List<String> allowedPrefixList = getAllowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isEmpty() || prefix.isBlank()) {
                throw new IllegalArgumentException("Header prefix cannot be blank");
            }
        }

        for (Method method : MessageHeaderConfiguration.class.getDeclaredMethods()) {
            if (isGetterMethod(method)) {
                if ("getAllowedPrefix".equals(method.getName())) {
                    continue;
                }
                Object value = method.invoke(this);
                if (value instanceof String stringValue) {
                    if (!startsWithValidPrefix(allowedPrefixList, stringValue)) {
                        throw new IllegalArgumentException(
                            method.getName() + " does not have a valid header value : " + stringValue
                        );
                    }
                }
            }
        }
    }

    private boolean isGetterMethod(Method method) {
        return method.getName().startsWith("get") && method.getReturnType() != void.class;
    }

    private boolean startsWithValidPrefix(List<String> allowedPrefixes, String value) {
        for (String prefix : allowedPrefixes) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

}
