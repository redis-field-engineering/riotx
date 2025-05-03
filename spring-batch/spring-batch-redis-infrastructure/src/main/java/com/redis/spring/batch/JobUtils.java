package com.redis.spring.batch;

import java.util.HashMap;
import java.util.Map;

import org.hsqldb.jdbc.JDBCDataSource;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties.Jdbc;
import org.springframework.boot.jdbc.init.DataSourceScriptDatabaseInitializer;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public abstract class JobUtils {

	private static final Map<String, JDBCDataSource> dataSources = new HashMap<>();

	private JobUtils() {
	}

	public static ResourcelessTransactionManager resourcelessTransactionManager() {
		return new ResourcelessTransactionManager();
	}

	public static JobRepositoryFactoryBean jobRepositoryFactoryBean(String name) throws Exception {
		JobRepositoryFactoryBean bean = new JobRepositoryFactoryBean();
		bean.setDataSource(hsqldbDataSource(name));
		bean.setDatabaseType("HSQL");
		bean.setTransactionManager(resourcelessTransactionManager());
		bean.afterPropertiesSet();
		return bean;
	}

	public static JDBCDataSource hsqldbDataSource(String databaseName) {
		return dataSources.computeIfAbsent(databaseName, JobUtils::newHsqldbDataSource);
	}

	public static JobExplorerFactoryBean jobExplorerFactoryBean(String name) throws Exception {
		JobExplorerFactoryBean bean = new JobExplorerFactoryBean();
		bean.setDataSource(hsqldbDataSource(name));
		bean.setTransactionManager(new DataSourceTransactionManager(hsqldbDataSource(name)));
		bean.afterPropertiesSet();
		return bean;
	}

	public static boolean isFailed(ExitStatus exitStatus) {
		return exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode());
	}

	private static JDBCDataSource newHsqldbDataSource(String databaseName) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL("jdbc:hsqldb:mem:" + databaseName);
		Jdbc jdbc = new Jdbc();
		jdbc.setInitializeSchema(DatabaseInitializationMode.ALWAYS);
		DataSourceScriptDatabaseInitializer initializer = new BatchDataSourceScriptDatabaseInitializer(dataSource,
				jdbc);
		initializer.initializeDatabase();
		return dataSource;
	}

}
