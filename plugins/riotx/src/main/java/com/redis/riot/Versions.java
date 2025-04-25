package com.redis.riot;

import java.io.ByteArrayOutputStream;

import picocli.CommandLine.IVersionProvider;

public class Versions implements IVersionProvider {

	@Override
	public String[] getVersion() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RiotVersion.banner(RiotUtils.newPrintStream(baos));
		return RiotUtils.toString(baos).split(System.lineSeparator());
	}
}