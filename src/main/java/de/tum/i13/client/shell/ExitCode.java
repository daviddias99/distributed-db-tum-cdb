package de.tum.i13.client.shell;

enum ExitCode {
    SUCCESS(20),
    COMMAND_NOT_FOUND(21),
    QUIT_PROGRAMM(22);

    private final int value;

    ExitCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
