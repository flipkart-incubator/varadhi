package com.flipkart.varadhi.entities.filters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.varadhi.entities.JsonMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class FilterSerializerAndDeserializerTest {
    private static ObjectMapper mapper;

    @BeforeAll
    public static void setUp() {
        mapper = JsonMapper.getMapper();
    }

    private static Stream<Arguments> provideConditions() {
        return createConditions(false);
    }

    private static Stream<Arguments> provideConditionsWithEmptyHeader() {
        return createConditions(true);
    }

    private static Stream<Arguments> createConditions(boolean emptyHeader) {
        return Stream.of(
            Arguments.of(
                "[{\"op\":\"AND\",\"values\":[{\"op\":\"startsWith\",\"key\":\"X_abc\",\"value\":\"my_prefix\"},{\"op\":\"endsWith\",\"key\":\"X_abc\",\"value\":\"suffix\"}]}]",
                false,
                "startsWith(X_abc,\"my_prefix\") and endsWith(X_abc,\"suffix\")"
            ),
            Arguments.of(
                "[{\"op\":\"AND\",\"values\":[{\"op\":\"startsWith\",\"key\":\"X_abc\",\"value\":\"my_prefix\"},{\"op\":\"endsWith\",\"key\":\"X_abc\",\"value\":\"prefix\"}]}]",
                !emptyHeader,
                "startsWith(X_abc,\"my_prefix\") and endsWith(X_abc,\"prefix\")"
            ),
            Arguments.of(
                "[{\"op\":\"OR\",\"values\":[{\"op\":\"contains\",\"key\":\"X_abc\",\"value\":\"substring\"},{\"op\":\"exists\",\"key\":\"X_abc\"}]}]",
                !emptyHeader,
                "contains(X_abc,\"substring\") or exists(X_abc)"
            ),
            Arguments.of(
                "[{\"op\":\"NAND\",\"values\":[{\"op\":\"startsWith\",\"key\":\"X_abc\",\"value\":\"my_prefix\"},{\"op\":\"endsWith\",\"key\":\"X_abc\",\"value\":\"suffix\"}]}]",
                true,
                "not(startsWith(X_abc,\"my_prefix\") and endsWith(X_abc,\"suffix\"))"
            ),
            Arguments.of(
                "[{\"op\":\"NOR\",\"values\":[{\"op\":\"contains\",\"key\":\"X_abc\",\"value\":\"substring\"},{\"op\":\"exists\",\"key\":\"X_abc\"}]}]",
                emptyHeader,
                "not(contains(X_abc,\"substring\") or exists(X_abc))"
            ),
            Arguments.of(
                "[{\"op\":\"NOT\",\"value\":{\"op\":\"startsWith\",\"key\":\"X_abc\",\"value\":\"my_prefix\"}}]",
                emptyHeader,
                "not(startsWith(X_abc,\"my_prefix\"))"
            ),
            Arguments.of(
                "[{\"op\":\"startsWith\",\"key\":\"X_abc\",\"value\":\"my_prefix\"}]",
                !emptyHeader,
                "startsWith(X_abc,\"my_prefix\")"
            ),
            Arguments.of(
                "[{\"op\":\"startsWith\",\"key\":\"X_abc\",\"value\":\"my_prefix_123\"}]",
                false,
                "startsWith(X_abc,\"my_prefix_123\")"
            ),
            Arguments.of(
                "[{\"op\":\"endsWith\",\"key\":\"X_abc\",\"value\":\"fix\"}]",
                !emptyHeader,
                "endsWith(X_abc,\"fix\")"
            ),
            Arguments.of(
                "[{\"op\":\"endsWith\",\"key\":\"X_abc\",\"value\":\"suffix\"}]",
                false,
                "endsWith(X_abc,\"suffix\")"
            ),
            Arguments.of(
                "[{\"op\":\"in\",\"key\":\"X_abc\",\"values\":[\"value1\",\"value2\",\"value3\"]}]",
                false,
                "in(X_abc,[\"value1\",\"value2\",\"value3\"])"
            ),
            Arguments.of(
                "[{\"op\":\"in\",\"key\":\"X_abc\",\"values\":[\"my_prefix\",\"my_prefix_2\",\"my_prefix_3\"]}]",
                !emptyHeader,
                "in(X_abc,[\"my_prefix\",\"my_prefix_2\",\"my_prefix_3\"])"
            ),
            Arguments.of(
                "[{\"op\":\"contains\",\"key\":\"X_abc\",\"value\":\"substring\"}]",
                false,
                "contains(X_abc,\"substring\")"
            ),
            Arguments.of(
                "[{\"op\":\"contains\",\"key\":\"X_abc\",\"value\":\"x\"}]",
                !emptyHeader,
                "contains(X_abc,\"x\")"
            ),
            Arguments.of("[{\"op\":\"exists\",\"key\":\"X_abc\"}]", !emptyHeader, "exists(X_abc)"),
            Arguments.of("[{\"op\":\"exists\",\"key\":\"X_abcd\"}]", false, "exists(X_abcd)")
        );
    }

    private static Stream<Arguments> provideMalformedConditions() {
        return Stream.of(
            Arguments.of("[{\"op\":\"UNKNOWN\",\"key\":\"X_abc\",\"value\":\"my_prefix\"}]"),
            Arguments.of("[{\"op\":\"AND\",\"values\":[{\"op\":\"startsWith\",\"key\":\"X_abc\"}]}]"),
            Arguments.of("[{\"op\":\"OR\",\"values\":[{\"op\":\"contains\",\"key\":\"X_abc\"}]}]"),
            Arguments.of("[{\"op\":\"NOT\",\"value\":{\"op\":\"unknown\",\"key\":\"X_abc\",\"value\":\"my_prefix\"}}]"),
            Arguments.of("[{\"op\":\"startsWith\",\"key\":\"X_abc\"}]")
        );
    }

    @ParameterizedTest
    @MethodSource ("provideConditions")
    public void testConditions(String json, boolean expected, String result) throws JsonProcessingException {
        List<Condition> conditions = mapper.readValue(
            json,
            mapper.getTypeFactory().constructCollectionType(List.class, Condition.class)
        );

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("X_abc", "my_prefix");
        headers.put("X_abc", "my_prefix1");
        headers.put("X_abc", "my_prefix2");

        for (Condition condition : conditions) {
            assertEquals(expected, condition.evaluate(headers));
            assertEquals(result, condition.toString());
        }
    }

    @ParameterizedTest
    @MethodSource ("provideConditionsWithEmptyHeader")
    public void testConditionsWithEmptyHeader(String json, boolean expected, String result)
        throws JsonProcessingException {
        List<Condition> conditions = mapper.readValue(
            json,
            mapper.getTypeFactory().constructCollectionType(List.class, Condition.class)
        );

        Multimap<String, String> headers = ArrayListMultimap.create();

        for (Condition condition : conditions) {
            assertEquals(expected, condition.evaluate(headers));
            assertEquals(result, condition.toString());
        }
    }

    @ParameterizedTest
    @MethodSource ("provideMalformedConditions")
    public void testMalformedConditions(String json) {
        ObjectMapper mapper = JsonMapper.getMapper();
        Executable executable = () -> mapper.readValue(
            json,
            mapper.getTypeFactory().constructCollectionType(List.class, Condition.class)
        );
        assertThrows(JsonProcessingException.class, executable);
    }
}
