package com.perficient.cassandra;

import java.io.IOException;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.xml.ClassPathXmlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 * 
 */
public class CassandraTestInstanceInitializer {
	private static final String LOAD_FILE = "column_schema.xml";
	private static final String LOAD_FILE_2 = "dataSetAllDataTypes.xml";
	public static final String KS_NAME = "TestKeyspace";
	public static final String CF_NAME = "10family";
	public static final int PORT = 9171;
	public static final String HOST = "localhost";
	public static final String HOST_AND_PORT = HOST + ":" + PORT;
	public static final String CLUSTER_NAME = "TestCluster";

	private static Logger LOG = LoggerFactory.getLogger(CassandraTestInstanceInitializer.class);

	
	public static void main(String[] args) {

		CassandraTestInstanceInitializer app = new CassandraTestInstanceInitializer();
		
		

	}

	public static void initializeTestCassandraInstance() {

		try {
			LOG.debug("starting the test Cassandra instance");
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();

			DataLoader dataLoader = new DataLoader(CLUSTER_NAME, HOST_AND_PORT);
			dataLoader.load(new ClassPathXmlDataSet(LOAD_FILE));

			Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME,
					HOST_AND_PORT);
			Keyspace keyspace = HFactory.createKeyspace(KS_NAME, cluster);
			LOG.debug("DONE starting the test Cassandra instance");


		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void stopTestCassandraInstance() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

		
	
	}

	
}