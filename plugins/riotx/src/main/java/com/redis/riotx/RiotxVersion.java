package com.redis.riotx;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ResourceBundle;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.RiotVersion;

public class RiotxVersion {

	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(RiotxVersion.class.getName());
	private static final String RIOTX_VERSION = BUNDLE.getString("riotx_version");
	private static final String RIOTX_FORMAT = "riotx %s%n";
	private static final String RIOTX_VERSION_FORMAT = "RIOTx%s";

	private RiotxVersion() {
		// noop
	}

	public static String getPlainVersion() {
		return RIOTX_VERSION;
	}

	public static String riotVersion() {
		return String.format(RIOTX_VERSION_FORMAT, RIOTX_VERSION);
	}

	public static void banner(PrintStream out) {
		banner(out, true);
	}

	public static void banner(PrintStream out, boolean full) {
		banner(RiotUtils.newPrintWriter(out), full);
	}

	public static void banner(PrintWriter out) {
		banner(out, true);
	}

	public static void banner(PrintWriter out, boolean full) {
		RiotVersion.banner(out, full, BUNDLE, RIOTX_FORMAT, RIOTX_VERSION);
	}

}