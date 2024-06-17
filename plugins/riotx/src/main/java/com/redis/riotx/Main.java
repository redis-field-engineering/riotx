package com.redis.riotx;

import java.io.PrintWriter;

import com.redis.riot.Compare;
import com.redis.riot.DatabaseExport;
import com.redis.riot.DatabaseImport;
import com.redis.riot.FakerImport;
import com.redis.riot.FileExport;
import com.redis.riot.FileImport;
import com.redis.riot.Generate;
import com.redis.riot.Ping;
import com.redis.riot.Replicate;
import com.redis.riot.core.BaseCommand;
import com.redis.riot.core.IO;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riotx", versionProvider = Versions.class, headerHeading = "RIOTX is a data import/export tool for Redis Enterprise and Redis Cloud.%n%n", subcommands = {
		DatabaseExport.class, DatabaseImport.class, FakerImport.class, FileExport.class, FileImport.class,
		Generate.class, Ping.class, Replicate.class, Compare.class, StreamExport.class, MemcachedReplicate.class,
		GenerateCompletion.class })
public class Main extends BaseCommand implements Runnable, IO {

	private PrintWriter out;
	private PrintWriter err;

	@Override
	public PrintWriter getOut() {
		return out;
	}

	@Override
	public void setOut(PrintWriter out) {
		this.out = out;
	}

	@Override
	public PrintWriter getErr() {
		return err;
	}

	@Override
	public void setErr(PrintWriter err) {
		this.err = err;
	}

	@Override
	public void run() {
		commandSpec.commandLine().usage(out);
	}

	public static void main(String[] args) {
		CommandLine commandLine = com.redis.riot.Main.commandLine(new Main());
		commandLine.registerConverter(InetSocketAddressList.class, InetSocketAddressList::parse);
		commandLine.setExecutionStrategy(com.redis.riot.Main::executionStrategy);
		System.exit(com.redis.riot.Main.run(commandLine, args));
	}

}
