
package filters;

import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.StringConditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringConditionsTest {
    @Test
    public void testContainsCondition() {
        Condition condition = new StringConditions.ContainsCondition("key1", "value1");

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "somevalue1");

        assertTrue(condition.evaluate(headers));

        headers.removeAll("key1");
        headers.put("key1", "othervalue");
        assertFalse(condition.evaluate(headers));
    }

    @Test
    public void testStartsWithCondition() {
        Condition condition = new StringConditions.StartsWithCondition("key1", "value");

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");

        assertTrue(condition.evaluate(headers));

        headers.removeAll("key1");
        headers.put("key1", "othervalue");
        assertFalse(condition.evaluate(headers));
    }

    @Test
    public void testEndsWithCondition() {
        Condition condition = new StringConditions.EndsWithCondition("key1", "value1");

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "somevalue1");

        assertTrue(condition.evaluate(headers));

        headers.removeAll("key1");
        headers.put("key1", "value2");
        assertFalse(condition.evaluate(headers));
    }

    @Test
    public void testExistsCondition() {
        Condition condition = new StringConditions.ExistsCondition("key1");

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");

        assertTrue(condition.evaluate(headers));

        headers.removeAll("key1");
        assertFalse(condition.evaluate(headers));
    }

    @Test
    public void testInCondition() {
        Condition condition = new StringConditions.InCondition("key1", List.of("value1", "value2"));

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");

        assertTrue(condition.evaluate(headers));

        headers.removeAll("key1");
        headers.put("key1", "value3");
        assertFalse(condition.evaluate(headers));
    }
}
