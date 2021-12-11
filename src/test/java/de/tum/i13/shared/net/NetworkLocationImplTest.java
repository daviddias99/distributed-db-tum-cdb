package de.tum.i13.shared.net;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;

class NetworkLocationImplTest {

    static final String MY_ADDRESS = "myAddress";
    static final int MY_PORT = 42;

    @Test
    void failsOnNullAddress() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new NetworkLocationImpl(null, MY_PORT))
                .withMessageContainingAll("address", "not be null");
    }

    @Test
    void failsOnPortTooHigh() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new NetworkLocationImpl(MY_ADDRESS, 999999))
                .withMessageContainingAll("Port number", "between");
    }

    @Nested
    class CorrectLocationTest {

        private NetworkLocationImpl location;

        @BeforeEach
        void createLocation() {
            location = new NetworkLocationImpl(MY_ADDRESS, MY_PORT);
        }

        @Test
        void getsAddress() {
            assertThat(location.getAddress())
                    .isEqualTo(MY_ADDRESS);
        }

        @Test
        void getsPort() {
            assertThat(location.getPort())
                    .isEqualTo(MY_PORT);
        }

        @Nested
        class WithOtherLocationTest {

            private NetworkLocationImpl otherLocation;

            @BeforeEach
            void createOtherLocation() {
                otherLocation = new NetworkLocationImpl(MY_ADDRESS, MY_PORT);
                assumeThat(location != otherLocation)
                        .withFailMessage("We didn't have different locations objects")
                        .isTrue();
            }

            @Test
            void testEquals() {
                assertThat(location)
                        .isEqualTo(otherLocation);
            }

            @Test
            void testHashCode() {
                assertThat(location)
                        .hasSameHashCodeAs(otherLocation);
            }

        }

        @Test
        void testToString() {
            assertThat(location)
                    .hasToString("NetworkLocationImpl{myAddress:42}");
        }

    }


}