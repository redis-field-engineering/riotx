package com.redis.riotx;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ResourceBundle;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.RiotVersion;

public class RiotxVersion {

	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(RiotxVersion.class.getName());
	private static final String RIOT_X_VERSION = BUNDLE.getString("riotx_version");
	private static final String RIOT_X_FORMAT = "riotx %s%n";

	private RiotxVersion() {
	}

	public static String getVersion() {
		return RIOT_X_VERSION;
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
		RiotVersion.banner(out, full, BUNDLE, RIOT_X_FORMAT, RIOT_X_VERSION);
	}

}