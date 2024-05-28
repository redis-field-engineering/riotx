package com.redis.riotx;

import java.io.ByteArrayOutputStream;

import com.redis.riot.core.RiotUtils;

import picocli.CommandLine.IVersionProvider;

public class Versions implements IVersionProvider {

	@Override
	public String[] getVersion() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RiotxVersion.banner(RiotUtils.newPrintStream(baos));
		return RiotUtils.toString(baos).split(System.lineSeparator());
	}
}