package com.perficient.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response.Status.Family;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import cassadra.stage.Keyspace;
import cassadra.stage.Keyspace.ColumnFamilies.ColumnFamily;
import cassadra.stage.Keyspace.ColumnFamilies.ColumnFamily.ColumnMetadata;
import cassadra.stage.Keyspace.ColumnFamilies.ColumnFamily.Row;
import cassadra.stage.Keyspace.ColumnFamilies.ColumnFamily.Row.Column;

public class Util {

	// private static final String FILE = "test.xml";
	private static final String FILE = "empty_shell.xml";

	public static void main(String[] args) {

		genFile();

	}

	private static void genFile() {
		try {
			JAXBContext context = JAXBContext.newInstance(Keyspace.class);
			Unmarshaller unMarshaller = context.createUnmarshaller();
			InputStream file = ClassLoader.getSystemResourceAsStream(FILE);

			Keyspace ks = (Keyspace) unMarshaller.unmarshal(file);
			if (ks.getColumnFamilies() != null
					&& ks.getColumnFamilies().getColumnFamily() != null
					&& ks.getColumnFamilies().getColumnFamily().size() > 0
					&& ks.getColumnFamilies().getColumnFamily().get(0) != null) {
				// info(ks);
				ks.getColumnFamilies().getColumnFamily().get(0)
						.getColumnMetadata().clear();
			}

			// populateRows(ks,1000);
			generateSchemaFile(ks, 10, 0);

			generateSchemaFile(ks, 1000, 1);

//			String outputFileName = "target/" + numberOfColumns
//					+ "_column_schema.xml";

			
			String outputFileName = "target/column_schema.xml";
			
			File outputFile = new File(outputFileName);

			Marshaller marshaller = context.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
					Boolean.TRUE);

			marshaller.marshal(ks, outputFile);

		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void generateSchemaFile(Keyspace ks, int numberOfColumns,
			int columnFamilyNumber) {

		// if (ks.getColumnFamilies().getColumnFamily().get(columnFamilyNumber)
		// == null){
		//
		// ColumnFamily cf = createCf(numberOfColumns + "family");
		// ks.getColumnFamilies().getColumnFamily().add(cf);
		// }

		ColumnFamily cf = createCf(numberOfColumns + "family");
		ks.getColumnFamilies().getColumnFamily().add(cf);

		for (int i = 0; i < numberOfColumns; i++) {

			ColumnMetadata colMetaData = new ColumnMetadata();
			colMetaData.setName("" + (i+1));
			colMetaData.setValidationClass("UTF8Type");
			ks.getColumnFamilies().getColumnFamily().get(columnFamilyNumber)
					.getColumnMetadata().add(colMetaData);

		}

	}

	private static ColumnFamily createCf(String name) {
		ColumnFamily cf = new ColumnFamily();
		cf.setType("STANDARD");
		cf.setKeyType("UTF8Type");
		cf.setComparatorType("UTF8Type");
		cf.setDefaultColumnValueType("UTF8Type");
		cf.setName(name);
		return cf;
	}

	// private static void populateRows(Keyspace ks, int numberOfRows) {
	//
	// for (int i = 0; i < numberOfRows; i++) {
	// Keyspace.ColumnFamilies.ColumnFamily.Row row = new Row();
	// row.setKey("" + i);
	//
	// for (int j = 0; j < columnNames.length; j++) {
	// Column e = new Column();
	// e.setName(columnNames[j]);
	// e.setValue(UUID.randomUUID().toString());
	// row.getColumn().add(e);
	//
	// }
	// ks.getColumnFamilies().getColumnFamily().get(0).getRow()
	// .add(i, row);
	//
	// }
	// }

	private static void info(Keyspace ks) {
		System.out.println(ks.getName());
		System.out.println(ks.getColumnFamilies().getClass().toString());
		ks.getColumnFamilies().getColumnFamily().get(0);
		System.out.println(ks.getColumnFamilies().getColumnFamily().get(0)
				.getRow().get(0).getKey());
		System.out.println(ks.getColumnFamilies().getColumnFamily().get(0)
				.getRow().get(1).getKey());

		System.out.println(ks.getColumnFamilies().getColumnFamily().get(0)
				.getRow().size());
	}

}
