package filters;

import java.util.List;

import com.flipkart.varadhi.entities.filters.BooleanConditions;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.StringConditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.BeforeAll;



import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BooleanConditionsTest {
    static Condition condition1;
    static Condition condition2;

    @BeforeAll
    public static void setUp() {
        condition1 = new StringConditions.ContainsCondition("key1", "value1");
        condition2 = new StringConditions.ContainsCondition("key2", "value2");
    }

    @Test
    public void testAndCondition() {

        BooleanConditions.AndCondition andCondition = new BooleanConditions.AndCondition(
            List.of(condition1, condition2)
        );

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");
        headers.put("key2", "value2");

        assertTrue(andCondition.evaluate(headers));

        headers.removeAll("key2");
        assertFalse(andCondition.evaluate(headers));
    }

    @Test
    public void testNandCondition() {

        BooleanConditions.NandCondition nandCondition = new BooleanConditions.NandCondition(
            List.of(condition1, condition2)
        );

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");
        headers.put("key2", "value2");

        assertFalse(nandCondition.evaluate(headers));

        headers.removeAll("key2");
        assertTrue(nandCondition.evaluate(headers));
    }

    @Test
    public void testOrCondition() {

        BooleanConditions.OrCondition orCondition = new BooleanConditions.OrCondition(List.of(condition1, condition2));

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");

        assertTrue(orCondition.evaluate(headers));

        headers.removeAll("key1");
        headers.put("key2", "value2");
        assertTrue(orCondition.evaluate(headers));

        headers.removeAll("key2");
        assertFalse(orCondition.evaluate(headers));
    }

    @Test
    public void testNorCondition() {

        BooleanConditions.NorCondition norCondition = new BooleanConditions.NorCondition(
            List.of(condition1, condition2)
        );

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");

        assertFalse(norCondition.evaluate(headers));

        headers.removeAll("key1");
        headers.put("key2", "value2");
        assertFalse(norCondition.evaluate(headers));

        headers.removeAll("key2");
        assertTrue(norCondition.evaluate(headers));
    }

    @Test
    public void testNotCondition() {

        BooleanConditions.NotCondition notCondition = new BooleanConditions.NotCondition(condition1);

        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put("key1", "value1");

        assertFalse(notCondition.evaluate(headers));

        headers.removeAll("key1");
        assertTrue(notCondition.evaluate(headers));
    }
}
