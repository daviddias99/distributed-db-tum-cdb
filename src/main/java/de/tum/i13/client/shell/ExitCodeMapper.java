package de.tum.i13.client.shell;

import picocli.CommandLine;

public class ExitCodeMapper implements CommandLine.IExitCodeExceptionMapper {

    @Override
    public int getExitCode(Throwable exception) {
        return ExitCode.UNKNOWN_EXCEPTION.getValue();
    }

}
