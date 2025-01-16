package com.redis.riotx;

import com.redis.riot.AbstractFileImport;
import com.redis.riot.Riot;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riotx", versionProvider = Versions.class, footerHeading = "%nRun 'riotx COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT-X, head to https://redis-field-engineering.github.io/riotx%n", subcommands = {
		RedisImportCommand.class, StreamImport.class, StreamExport.class, MemcachedReplicate.class, SnowflakeImport.class })
public class Riotx extends Riot {

	public static void main(String[] args) {
		System.exit(new Riotx().run(args));
	}

	@Override
	protected CommandLine commandLine() {
		CommandLine commandLine = super.commandLine();
		commandLine.registerConverter(InetSocketAddressList.class, InetSocketAddressList::parse);
		return commandLine;
	}

	@Override
	protected ReplicateX replicate() {
		return new ReplicateX();
	}

	@Override
	protected AbstractFileImport fileImport() {
		return new FileImportX();
	}

}
