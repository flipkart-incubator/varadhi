package com.flipkart.varadhi.web.subscription;


import com.flipkart.varadhi.web.config.RestOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.flipkart.varadhi.entities.Constants.SubscriptionProperties.*;

public record SubscriptionPropertyValidator(
    Function<String, Boolean> restrictiveValidator,
    Function<String, Boolean> permissibleValidator
) {
    public static Map<String, String> createPropertyDefaultValueProviders(RestOptions restOptions) {
        Map<String, String> propertyDefaultValueProviders = new HashMap<>();
        propertyDefaultValueProviders.put(
            UNSIDELINE_API_MESSAGE_COUNT,
            String.valueOf(restOptions.getUnsidelineApiMsgCountDefault())
        );
        propertyDefaultValueProviders.put(
            UNSIDELINE_API_GROUP_COUNT,
            String.valueOf(restOptions.getUnsidelineApiGroupCountDefault())
        );
        propertyDefaultValueProviders.put(
            GETMESSAGES_API_MESSAGES_LIMIT,
            String.valueOf(restOptions.getGetMessagesApiMessagesLimitDefault())
        );
        return propertyDefaultValueProviders;
    }

    public static Map<String, SubscriptionPropertyValidator> createPropertyValidators(RestOptions restOptions) {
        Map<String, SubscriptionPropertyValidator> validators = new HashMap<>();
        validators.put(
            UNSIDELINE_API_MESSAGE_COUNT,
            new SubscriptionPropertyValidator(
                isInRange(0, restOptions.getUnsidelineApiMsgCountMax()),
                isEqualOrHigher(0)
            )
        );
        validators.put(
            UNSIDELINE_API_GROUP_COUNT,
            new SubscriptionPropertyValidator(
                isInRange(0, restOptions.getUnsidelineApiGroupCountMax()),
                isEqualOrHigher(0)
            )
        );
        validators.put(
            GETMESSAGES_API_MESSAGES_LIMIT,
            new SubscriptionPropertyValidator(
                isInRange(0, restOptions.getGetMessagesApiMessagesLimitMax()),
                isEqualOrHigher(0)
            )
        );
        return validators;
    }

    private static Function<String, Boolean> isInRange(int minValue, int maxValue) {
        return propValue -> {
            int value = Integer.parseInt(propValue);
            return value >= minValue && value <= maxValue;
        };
    }

    private static Function<String, Boolean> isEqualOrHigher(int minValue) {
        return propValue -> {
            int value = Integer.parseInt(propValue);
            return value >= minValue;
        };
    }

    public boolean isValid(String value, boolean usePermissible) {
        Function<String, Boolean> validator = usePermissible ? permissibleValidator : restrictiveValidator;
        return validator == null || validator.apply(value);
    }
}
