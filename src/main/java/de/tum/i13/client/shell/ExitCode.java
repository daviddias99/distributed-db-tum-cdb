package de.tum.i13.client.shell;

enum ExitCode {
    SUCCESS(20),
    COMMAND_PARSING_FAILED(21),
    QUIT_PROGRAMM(22),
    CLIENT_EXCEPTION(23),
    UNKNOWN_EXCEPTION(24);

    private final int value;

    ExitCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
