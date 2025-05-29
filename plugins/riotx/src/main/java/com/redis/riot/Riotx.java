package com.redis.riot;

import com.redis.riot.operation.OperationCommand;
import com.redis.riot.replicate.Replicate;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riotx", versionProvider = Versions.class, subcommands = { DatabaseExport.class, DatabaseImport.class,
        FakerImport.class, FileExport.class, FileImport.class, Generate.class, Ping.class, Replicate.class, Compare.class,
        GenerateCompletion.class, RedisImport.class, StreamImport.class, StreamExport.class, MemcachedReplicate.class,
        SnowflakeImport.class,
        Stats.class }, description = "RIOT-X: Get data in and out of Redis.", footerHeading = "%nRun 'riotx COMMAND --help' for more information on a command.%n%nFor more help on how to use RIOT-X, head to https://redis-field-engineering.github.io/riotx%n")
public class Riotx extends MainCommand {

    @Override
    protected int executionStrategy(CommandLine.ParseResult parseResult) {
        for (CommandLine.ParseResult subcommand : parseResult.subcommands()) {
            Object command = subcommand.commandSpec().userObject();
            if (AbstractImport.class.isAssignableFrom(command.getClass())) {
                AbstractImport importCommand = (AbstractImport) command;
                for (CommandLine.ParseResult redisCommand : subcommand.subcommands()) {
                    if (redisCommand.isUsageHelpRequested()) {
                        return new CommandLine.RunLast().execute(redisCommand);
                    }
                    OperationCommand operationCommand = (OperationCommand) redisCommand.commandSpec().userObject();
                    importCommand.getImportOperationCommands().add(operationCommand);
                }
                return new CommandLine.RunFirst().execute(subcommand);
            }
        }
        return new CommandLine.RunLast().execute(parseResult); // default execution strategy
    }

    public static void main(String[] args) {
        System.exit(new Riotx().run(args));
    }

}
