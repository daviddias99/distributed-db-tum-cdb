package de.tum.i13.shared;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.RetryConfig;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Constants {

    public static final Charset TELNET_ENCODING = StandardCharsets.ISO_8859_1; // encoding for telnet
    public static final int BYTES_PER_KB = 1024;
    public static final int MAX_MESSAGE_SIZE_KB = 128;
    public static final int MAX_MESSAGE_SIZE_BYTES = MAX_MESSAGE_SIZE_KB * BYTES_PER_KB;
    public static final int MAX_KEY_SIZE_BYTES = 20;
    public static final int MAX_VALUE_SIZE_KB = 120;
    public static final int MAX_VALUE_SIZE_BYTES = MAX_VALUE_SIZE_KB * BYTES_PER_KB;
    public static final int MAX_REQUEST_RETRIES = RetryConfig.DEFAULT_MAX_ATTEMPTS;
    public static final long EXP_BACKOFF_INIT_INTERVAL = IntervalFunction.DEFAULT_INITIAL_INTERVAL;
    public static final double EXP_BACKOFF_MULTIPLIER = IntervalFunction.DEFAULT_MULTIPLIER;
    public static final double EXP_BACKOFF_RAND_FACTOR = IntervalFunction.DEFAULT_RANDOMIZATION_FACTOR;
    public static final String LOGS_DIR = "logs";
    public static final String PROMPT = "EchoClient> ";
    public static final String CONNECT_COMMAND = "connect";
    public static final String DISCONNECT_COMMAND = "disconnect";
    public static final String PUT_COMMAND = "put";
    public static final String GET_COMMAND = "get";
    public static final String HELP_COMMAND = "help";
    public static final String QUIT_COMMAND = "quit";
    public static final String LOG_COMMAND = "logLevel";
    public static final String TERMINATING_STR = "\r\n";
    public static final String THROWING_EXCEPTION_LOG_MESSAGE = "Throwing exception";
    public static final int CORE_POOL_SIZE = 3;
    public static final int SERVER_POOL_SIZE = 3;
    public static final int HEARTBEAT_TIMEOUT_MILLISECONDS = 700;
    public static final int SECONDS_PER_PING = 1;
    private Constants() {
    }

}
