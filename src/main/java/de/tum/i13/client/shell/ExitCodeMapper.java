package de.tum.i13.client.shell;

import picocli.CommandLine;

/**
 * Maps exceptions in the {@link CLICommands} to an {@link ExitCode}.
 */
public class ExitCodeMapper implements CommandLine.IExitCodeExceptionMapper {

    @Override
    public int getExitCode(Throwable exception) {
        return ExitCode.UNKNOWN_EXCEPTION.getValue();
    }

}
