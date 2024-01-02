package com.flipkart.varadhi;

import com.flipkart.varadhi.entities.ValidateMetaStoreEntity;
import com.flipkart.varadhi.entities.MetaStoreEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ValidateMetaStoreEntityTest {

    @Test
    public void testName() {
        List.of("abc", "a-c", "a_c", "a12", "a_1", "a-1", "asdasdasdaasdasdad", "ab12sdfsdf")
                .forEach(name -> {
                    Assertions.assertDoesNotThrow(
                            () -> new TypeDefault(name, 0).validate(),
                            String.format("TypeDefault Failed for %s", name)
                    );
                    Assertions.assertDoesNotThrow(
                            () -> new Type2(name, 0).validate(),
                            String.format("Type2 Failed for %s", name)
                    );
                });

        List.of("", "a", "asdasdassdfsfsddfdfsdfsfsdfsfsdfsfdaasdasdad", "!!!", "12sadsad")
                .forEach(name ->
                        Assertions.assertDoesNotThrow(
                                () -> new Type2(name, 0).validate(),
                                String.format("Type2 Failed for %s", name)
                        )
                );

        Assertions.assertDoesNotThrow(() -> new Type2(null, 0).validate(), "Type2 Failed for null");


        List.of(
                        "1", "A", "a", "aacvdcv_", "dfsfds-", ".asdads", "-asdads", "_asdads",
                        "asdaAsdas", "asdasdasdaasdasdadsadasadadasasdadsad"
                )
                .forEach(name -> Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new TypeDefault(name, 0).validate(),
                        String.format("TypeDefault Failed for %s", name)
                ));

        IllegalArgumentException e =
                Assertions.assertThrows(IllegalArgumentException.class, () -> new TypeMessage("A", 0).validate(),
                        String.format("TypeMessage Failed for name")
                );
        Assertions.assertEquals("Custom message", e.getMessage());

        List.of("abc", "asdasdas", "asdasdasaa")
                .forEach(name ->
                        Assertions.assertDoesNotThrow(
                                () -> new TypeMax(name, 0).validate(),
                                String.format("Type2 Failed for %s", name)
                        )
                );

        List.of(
                        "a", "aaaaaaaaaaaa"
                )
                .forEach(name -> Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new TypeMax(name, 0).validate(),
                        String.format("TypeDefault Failed for %s", name)
                ));

    }

    @ValidateMetaStoreEntity
    public static class TypeDefault extends MetaStoreEntity {
        public TypeDefault(String name, int version) {
            super(name, version);
        }
    }

    @ValidateMetaStoreEntity(allowNullOrBlank = true, min = -1, max = -1, regexp = "")
    public static class Type2 extends MetaStoreEntity {
        public Type2(String name, int version) {
            super(name, version);
        }
    }

    @ValidateMetaStoreEntity(message = "Custom message")
    public static class TypeMessage extends MetaStoreEntity {
        public TypeMessage(String name, int version) {
            super(name, version);
        }
    }

    @ValidateMetaStoreEntity(max = 10)
    public static class TypeMax extends MetaStoreEntity {
        public TypeMax(String name, int version) {
            super(name, version);
        }
    }
}
