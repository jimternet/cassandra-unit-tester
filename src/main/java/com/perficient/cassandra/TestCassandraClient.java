package com.perficient.cassandra;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

public class TestCassandraClient {

	public static void main(String[] args) {

		LOG.debug("main starting");

		CassandraTestInstanceInitializer.initializeTestCassandraInstance();
		
		TestCassandraClient testClient = new TestCassandraClient();
		testClient.init();
		testClient.loadData(1000);
		LOG.info("DONE WITH LOAD");
		testClient.allRows();
		CassandraTestInstanceInitializer.stopTestCassandraInstance();

		
	}

	private Keyspace keyspace;
	private static Logger LOG = LoggerFactory.getLogger(TestCassandraClient.class);

	private ColumnFamily<String, String> COLUMN_FAMILY;
	
	private void init() {
		
		LOG.debug("init astynax CF");
		COLUMN_FAMILY = ColumnFamily
	            .newColumnFamily(CassandraTestInstanceInitializer.CF_NAME, StringSerializer.get(),
	                    StringSerializer.get());
		
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster(CassandraTestInstanceInitializer.CLUSTER_NAME)
				.forKeyspace(CassandraTestInstanceInitializer.KS_NAME)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(9160).setMaxConnsPerHost(1)
								.setSeeds(CassandraTestInstanceInitializer.HOST_AND_PORT))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		keyspace = context.getClient();
		LOG.debug("DONE init astynax CF");

	}
	
	private void loadData(int numberOfRows){
		MutationBatch m = keyspace.prepareMutationBatch();
		
		
		
		for (int rowCounter = 0; rowCounter < numberOfRows ; rowCounter++) {
			for (int colIterator = 1; colIterator < 11; colIterator++) {
				m.withRow(COLUMN_FAMILY, rowCounter + "")
			    .putColumn(colIterator + "", UUID.randomUUID().toString(), null);

			}
		}

		
		try {
		    OperationResult<Void> result = m.execute();
		} catch (ConnectionException e) {
		    LOG.error(e.toString());
		}

	}
	
	
	
	
	//Hack code to print the rows and columns. :p
	private void allRows(){
		try {
		    OperationResult<Rows<String, String>> rows = keyspace.prepareQuery(COLUMN_FAMILY)
			.getAllRows()
			.setRowLimit(100)  // This is the page size
			.withColumnRange(new RangeBuilder().setMaxSize(10).build())
			.setExceptionCallback(new ExceptionCallback() {
		            public boolean onException(ConnectionException e) {
		                return true;
		            }
			})
			.execute();
		    for (Row<String, String> row : rows.getResult()) {
				LOG.info("######################################################");

				LOG.info("ROW :: Key : " + row.getKey() + " : this many columns : " + row.getColumns().size());
				ColumnList<String> columns = row.getColumns();
				Collection<String> names = columns.getColumnNames();
		
				Iterator it = names.iterator();
				
				while (it.hasNext()){
					
					String columnName = (String) it.next();
					Column<String> column = columns.getColumnByName(columnName);
					LOG.info(columnName + " : " + column.getStringValue());
				}

				
				LOG.info("######################################################");


		    }
		} catch (ConnectionException e) {
		    Assert.fail();
		}
	}

}
