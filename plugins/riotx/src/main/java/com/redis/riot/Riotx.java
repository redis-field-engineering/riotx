package com.redis.riot;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riotx", versionProvider = Versions.class, subcommands = { DatabaseExport.class, DatabaseImport.class,
		FakerImport.class, FileExport.class, FileImport.class, Generate.class, Ping.class, Replicate.class,
		Compare.class, GenerateCompletion.class, RedisImportCommand.class, StreamImport.class, StreamExport.class,
		MemcachedReplicate.class, SnowflakeImport.class,
		Stats.class }, description = "RIOT-X: Get data in and out of Redis.", footerHeading = "%nRun 'riotx COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT-X, head to https://redis-field-engineering.github.io/riotx%n")
public class Riotx extends RiotMainCommand {

	public static void main(String[] args) {
		System.exit(new Riotx().run(args));
	}

	@Override
	protected void registerConverters(CommandLine commandLine) {
		super.registerConverters(commandLine);
		commandLine.registerConverter(InetSocketAddressList.class, InetSocketAddressList::parse);
	}

}
