package de.tum.i13.client.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.PrintWriter;

class ParameterExceptionHandler implements CommandLine.IParameterExceptionHandler {

    private static final Logger LOGGER = LogManager.getLogger(ParameterExceptionHandler.class);

    @Override
    public int handleParseException(CommandLine.ParameterException ex, String[] args) {
        LOGGER.info("Command parsing failed", ex);

        final CommandLine cmd = ex.getCommandLine();
        final PrintWriter err = cmd.getErr();
        final CommandLine.Help.ColorScheme colorScheme = cmd.getColorScheme();

        err.println(colorScheme.errorText(ex.getMessage()));
        cmd.usage(err, colorScheme);
        return ExitCode.COMMAND_PARSING_FAILED.getValue();
    }

}
