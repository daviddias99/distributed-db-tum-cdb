package de.tum.i13.server.kv;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class KVMessageTest {

    @Test
    void doesNotUnpackEmpty() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> KVMessage.unpackMessage(""))
                .withMessageContaining("Could not convert")
                .withMessageContaining(KVMessage.class.getSimpleName());
    }

    @Test
    void doesNotUnpackUnknownCommands() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> KVMessage.unpackMessage("this_is_a_very_weird_command"))
                .withMessageContaining("No enum constant");
    }

    @Test
    void unpacksZeroArguments() {
        assertThat(KVMessage.unpackMessage("server_stopped"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        null,
                        null,
                        KVMessage.StatusType.SERVER_STOPPED
                );
    }

    @Test
    void unpacksOneArgument() {
        assertThat(KVMessage.unpackMessage("get myKey"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "myKey",
                        null,
                        KVMessage.StatusType.GET
                );
    }

    @Test
    void unpacksTwoArguments() {
        assertThat(KVMessage.unpackMessage("put myKey myValue"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "myKey",
                        "myValue",
                        KVMessage.StatusType.PUT
                );
    }

    @Test
    void doesNotUnpackFourArguments() {
        assertThat(KVMessage.unpackMessage("put myKey to my value"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "myKey",
                        "to my value",
                        KVMessage.StatusType.PUT
                );
    }

    @Test
    void respectsMultipleSpacesInValue() {
        assertThat(KVMessage.unpackMessage("put myKey to     my     value"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "myKey",
                        "to     my     value",
                        KVMessage.StatusType.PUT
                );
    }

    @Test
    void ignoresMultipleSpacesBetweenTokens() {
        assertThat(KVMessage.unpackMessage("put       myKey         to my value"))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "myKey",
                        "to my value",
                        KVMessage.StatusType.PUT
                );
    }

    @Test
    void trimsSpacesAtStartAndEnd() {
        assertThat(KVMessage.unpackMessage("    put myKey myValue    "))
                .extracting(
                        KVMessage::getKey,
                        KVMessage::getValue,
                        KVMessage::getStatus)
                .containsExactly(
                        "myKey",
                        "myValue",
                        KVMessage.StatusType.PUT
                );
    }

    @Test
    void packsUndefinedError() {
        final KVMessage message = spy(KVMessage.class);
        when(message.getKey()).thenReturn(null);
        when(message.getValue()).thenReturn(null);
        when(message.getStatus()).thenReturn(KVMessage.StatusType.ERROR);

        assertThat(message.packMessage())
                .isEqualTo("error");
    }

    @Test
    void packsWithZeroArguments() {
        final KVMessage message = spy(KVMessage.class);
        when(message.getKey()).thenReturn(null);
        when(message.getValue()).thenReturn(null);
        when(message.getStatus()).thenReturn(KVMessage.StatusType.SERVER_STOPPED);

        assertThat(message.packMessage())
                .isEqualTo("server_stopped");
    }

    @Test
    void packsWithOneArguments() {
        final KVMessage message = spy(KVMessage.class);
        when(message.getKey()).thenReturn("myKey");
        when(message.getValue()).thenReturn(null);
        when(message.getStatus()).thenReturn(KVMessage.StatusType.GET);

        assertThat(message.packMessage())
                .isEqualTo("get myKey");
    }

    @Test
    void packsWithTwoArguments() {
        final KVMessage message = spy(KVMessage.class);
        when(message.getKey()).thenReturn("myKey");
        when(message.getValue()).thenReturn("myValue");
        when(message.getStatus()).thenReturn(KVMessage.StatusType.PUT);

        assertThat(message.packMessage())
                .isEqualTo("put myKey myValue");
    }

}