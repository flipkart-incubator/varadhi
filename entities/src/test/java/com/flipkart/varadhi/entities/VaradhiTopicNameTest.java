package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VaradhiTopicNameTest {

    @Test
    void parse_ValidFqn_ReturnsParts() {
        VaradhiTopicName name = VaradhiTopicName.parse("myProject.myTopic");
        assertAll(
            () -> assertEquals("myProject", name.getProjectName()),
            () -> assertEquals("myTopic", name.getTopicName())
        );
    }

    @Test
    void toFqn_RoundTripsWithParse() {
        VaradhiTopicName original = VaradhiTopicName.of("p", "t");
        assertEquals(original, VaradhiTopicName.parse(original.toFqn()));
    }

    @ParameterizedTest
    @ValueSource (strings = {"", "nodot", "onlyproject.", ".onlytopic", "a..b", "a.b.c", "  ", " \t.\t "})
    void parse_InvalidFormat_Throws(String fqn) {
        assertThrows(IllegalArgumentException.class, () -> VaradhiTopicName.parse(fqn));
    }
}
