package de.tum.i13.client.shell;

enum ExitCode {
    SUCCESS(0),
    COMMAND_NOT_FOUND(1),
    QUIT_PROGRAMM(2);

    private final int value;

    ExitCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
