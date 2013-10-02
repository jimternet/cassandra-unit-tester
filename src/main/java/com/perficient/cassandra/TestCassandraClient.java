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

	private Keyspace keyspace;
	private static Logger LOG = LoggerFactory.getLogger(TestCassandraClient.class);
	
	
	
	public static void main(String[] args) {

		LOG.debug("main starting");

		
		TestCassandraClient testClient = new TestCassandraClient();
		loadTenColumns(testClient);
		loadHundredColumns(testClient);
		loadThousandColumns(testClient);
//		testClient.allRows();

		
	}


	private static void loadTenColumns(TestCassandraClient testClient) {
		int columnFamilySize = 10;

		ColumnFamily<String, String> columnFamily = testClient.init(columnFamilySize);
		testClient.loadData(1000, columnFamilySize, columnFamily);
		LOG.info("DONE WITH LOAD");
	}
	
	private static void loadHundredColumns(TestCassandraClient testClient) {
		int columnFamilySize = 100;

		ColumnFamily<String, String> columnFamily = testClient.init(columnFamilySize);
		testClient.loadData(1000, columnFamilySize, columnFamily);
		LOG.info("DONE WITH LOAD");
	}
	
	private static void loadThousandColumns(TestCassandraClient testClient) {
		int columnFamilySize = 1000;
		ColumnFamily<String, String> columnFamily = testClient.init(columnFamilySize);
		testClient.loadData(1000, columnFamilySize, columnFamily);
		LOG.info("DONE WITH LOAD");
	}




//	private ColumnFamily<String, String> COLUMN_FAMILY;
	
	public ColumnFamily<String, String> init(int numberOfColumns) {
		
		LOG.debug("init astynax CF");
		 ColumnFamily<String, String> columnFamily = ColumnFamily
	            .newColumnFamily(numberOfColumns+"family", StringSerializer.get(),
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
		LOG.info("DONE init astynax CF");
		return columnFamily;

	}
	
	private void loadData(int numberOfRows, int numberOfColumns, ColumnFamily<String, String> columnFamily){
		
	    LOG.info("preparing to load into : " + columnFamily.getName());

		
		for (int rowCounter = 0; rowCounter < numberOfRows ; rowCounter++) {
			//this is one row at a time.
			MutationBatch m = keyspace.prepareMutationBatch();

			for (int colIterator = 1; colIterator < numberOfColumns +1; colIterator++) {
				m.withRow(columnFamily, rowCounter + "")
			    .putColumn(colIterator + "", UUID.randomUUID().toString(), null);

			}
			try {
			    OperationResult<Void> result = m.execute();
			} catch (ConnectionException e) {
			    LOG.error(e.toString());
			    e.printStackTrace();
			}

		}

		

	}
	
	private void loadDataBigBatch(int numberOfRows, int numberOfColumns, ColumnFamily<String, String> columnFamily){
		MutationBatch m = keyspace.prepareMutationBatch();
		
	    LOG.info("preparing to load into : " + columnFamily.getName());

		
		for (int rowCounter = 0; rowCounter < numberOfRows ; rowCounter++) {
			for (int colIterator = 1; colIterator < numberOfColumns +1; colIterator++) {
				m.withRow(columnFamily, rowCounter + "")
			    .putColumn(colIterator + "", UUID.randomUUID().toString(), null);

			}
		}

		
		try {
		    OperationResult<Void> result = m.execute();
		} catch (ConnectionException e) {
		    LOG.error(e.toString());
		    e.printStackTrace();
		}

	}
	
	
	//Hack code to print the rows and columns. :p
	private void allRows(ColumnFamily<String, String> columnFamily){
		try {
		    OperationResult<Rows<String, String>> rows = keyspace.prepareQuery(columnFamily)
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


	public Keyspace getKeyspace() {
		return keyspace;
	}


	public void setKeyspace(Keyspace keyspace) {
		this.keyspace = keyspace;
	}
	
	
	

}
