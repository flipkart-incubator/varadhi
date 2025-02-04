package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.config.MessageHeaderConfiguration;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.List;

@Slf4j
public class PrefixBasedHeaderValidator implements ConstraintValidator<ValidateHeaderPrefix, MessageHeaderConfiguration> {

    /**
     * Implements the validation logic
     *
     * @param config  object to validate
     * @param context context in which the constraint is evaluated
     *
     * @return {@code false} if {@code value} does not pass the constraint
     */
    @Override
    public boolean isValid(MessageHeaderConfiguration config, ConstraintValidatorContext context) {
        List<String> allowedPrefixList = config.getAllowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isEmpty() || prefix.isBlank()) {
                context.buildConstraintViolationWithTemplate("Header 'allowedPrefix' cannot be null or empty.")
                        .addConstraintViolation();
                return false;
            }
        }
        for (Field field : MessageHeaderConfiguration.class.getDeclaredFields()) {
            try {
                field.setAccessible(true);
                if ("allowedPrefix".equals(field.getName())) {
                    continue;
                }
                Object value = field.get(config);

                if (value instanceof String stringValue) {
                    if (!startsWithValidPrefix(allowedPrefixList, stringValue)) {
                        context.buildConstraintViolationWithTemplate(
                                String.format(
                                        "Header '%s' with value '%s' doesn't start with any valid prefix",
                                        field.getName(), stringValue
                                )
                        ).addPropertyNode(field.getName()).addConstraintViolation();
                        return false;
                    }
                }
            } catch (IllegalAccessException e) {
                log.error(e.getMessage());
                return false;
            }
        }

        return true;
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

