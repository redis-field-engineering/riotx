package com.redis.riotx;

import com.redis.riot.Compare;
import com.redis.riot.DatabaseExport;
import com.redis.riot.DatabaseImport;
import com.redis.riot.FakerImport;
import com.redis.riot.Generate;
import com.redis.riot.Ping;
import com.redis.riot.RiotMainCommand;

import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riotx", versionProvider = Versions.class, subcommands = { DatabaseExport.class, DatabaseImport.class,
		FakerImport.class, FileExportX.class, FileImportX.class, Generate.class, Ping.class, ReplicateX.class,
		Compare.class, GenerateCompletion.class, RedisImportCommand.class, StreamImport.class, StreamExport.class,
		MemcachedReplicate.class, SnowflakeImport.class,
		Stats.class }, description = "RIOT extension for the Enterprise.", footerHeading = "%nRun 'riotx COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT-X, head to https://redis-field-engineering.github.io/riotx%n")
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
