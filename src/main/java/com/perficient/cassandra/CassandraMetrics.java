package com.perficient.cassandra;

import java.util.Random;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;

public class CassandraMetrics {

	Random randomGenerator = new Random();
	
	
	public static void main(String[] args) {

		CassandraMetrics metrics = new CassandraMetrics();
		long startK = System.currentTimeMillis();
		metrics.timeAgainstColumnFamily(1000, 1000);
		long stopK = System.currentTimeMillis();
		long startTen = System.currentTimeMillis();
		metrics.timeAgainstColumnFamily(10, 1000);
		long stopTen = System.currentTimeMillis();

		System.out.println("10 cols took : " + (stopTen-startTen));
		System.out.println("1000 cols took : " + (stopK-startK));


		
	}

	private void timeAgainstColumnFamily(int columnFamilySize, int numberOfRows) {
		TestCassandraClient testClient = new TestCassandraClient();
		ColumnFamily<String, String> columnFamily = testClient
				.init(columnFamilySize);

		for (int i = 0; i < 10000; i++) {
			String rowKey = randomGenerator.nextInt(numberOfRows) + "";
			ColumnList<String> result = null;
			try {
				result = testClient.getKeyspace().prepareQuery(columnFamily)
						.getKey(rowKey).execute().getResult();
			} catch (ConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (!result.isEmpty()) {
			}
		}

	}

}
