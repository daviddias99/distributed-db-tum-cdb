package de.tum.i13.shared;

public class Constants {
	public static final String TELNET_ENCODING = "ISO-8859-1"; // encoding for telnet
	public static final int BYTES_PER_KB = 1024;
	public static final int MAX_MESSAGE_SIZE_KB = 128;
	public static final int MAX_MESSAGE_SIZE_BYTES = MAX_MESSAGE_SIZE_KB * BYTES_PER_KB;
	public static final String LOGS_DIR = "logs";

	public static final String PROMPT = "EchoClient> ";
	public static final String CONNECT_COMMAND = "connect";
	public static final String DISCONNECT_COMMAND = "disconnect";
	public static final String HELP_COMMAND = "help";
	public static final String QUIT_COMMAND = "quit";
	public static final String SEND_COMMAND = "send";
	public static final String LOG_COMMAND = "logLevel";
	public static final String TERMINATING_STR = "\r\n";
}
