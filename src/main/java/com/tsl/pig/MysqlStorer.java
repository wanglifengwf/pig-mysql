package com.tsl.pig;

import java.io.IOException;

import com.tsl.hadoop.MysqlConfiguration;
import com.tsl.hadoop.MysqlOutputFormat;
import com.tsl.hadoop.MysqlRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;

import com.tsl.hadoop.MysqlRecordWriter;
import org.joda.time.DateTime;

public class MysqlStorer extends StoreFunc implements StoreMetadata {

	private static final Log LOG = LogFactory.getLog(MysqlStorer.class);
	private String [] hostnames = null;
	private String database = null;
	private String port = null;
	private String username = null;
	private String password = null;
	
	private MysqlOutputFormat outputFormat = new MysqlOutputFormat();

	private String tableName = null;
	//private String tableDef = null;
	private String sql = null;

	private MysqlRecordWriter writer = null;

	public MysqlStorer(String hostnames, String db, String port, String username,
			String password) {
		this.hostnames = hostnames.split(",");
		this.database = db;
		this.port = port;
		this.username = username;
		this.password = password;
	}

	//Important to override because of PIG-1378
	public String relToAbsPathForStorage(String location, Path curdir) {
		return location;
	}
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return outputFormat;
    }

	@Override
	public void prepareToWrite(RecordWriter writer) {
		this.writer = (MysqlRecordWriter) writer;
	}

	@Override
	public void setStoreLocation(String location, Job job) 
			throws IOException {
		String[] locs = location.split("\\{");
	    String tableNameDef = locs[locs.length - 1];
    	tableNameDef = tableNameDef.substring(0, tableNameDef.lastIndexOf('}'));

		tableName = tableNameDef;
		int cols = tableNameDef.indexOf('(');
		if(cols != -1) {
			tableName = tableNameDef.substring(0, cols);
			sql = tableNameDef.substring(cols + 1, tableNameDef.length() - 1);
			LOG.info(tableName + " setStoreLocation sql : " + sql);
		}
		Configuration conf = job.getConfiguration();
		MysqlConfiguration.configureVertica(conf, hostnames, database, port, username, password);
		if (sql != null) {
			outputFormat.setOutput(job, tableName, false, sql);
		} else {
			outputFormat.setOutput(job, tableName);
		}
	}

	@Override
	public void checkSchema(ResourceSchema schema) {
		//TODO: Check column data types too.
	}

	public void storeSchema(ResourceSchema schema, String location, Job job) {
	}

	public void storeStatistics(ResourceStatistics stats, String location, Job job) {
	}
  	
	@Override
	public void putNext(Tuple f) throws IOException,org.apache.pig.backend.executionengine.ExecException {
		MysqlRecord record = new MysqlRecord();
		for(int i = 0; i < f.size(); i++) {
			Object obj = f.get(i);
			switch (f.getType(i)) {
				case DataType.BOOLEAN:
					record.add((Boolean)obj);
					break;
				case DataType.FLOAT:
					record.add((Float)obj);
					break;
				case DataType.DOUBLE:
					record.add((Double)obj);
					break;
				case DataType.LONG:
				case DataType.INTEGER:
					record.add(new Long(((Number)obj).longValue()));
					break;
				case DataType.BYTE:
				case DataType.BYTEARRAY:
					record.add(((DataByteArray)obj).get());
					break;
				case DataType.CHARARRAY:
				case DataType.BIGCHARARRAY:
					record.add((String)obj);
					break;
				case DataType.NULL: 
					record.add(null);
					break;
				case DataType.DATETIME:
					record.add(((DateTime)obj).toString("yyyy-MM-dd HH-mm-ss"));
					break;
				default:
					throw new IOException("Vertica connector does not support " 
							+ DataType.findTypeName(f.getType(i)));
			}
		}
		writer.write(null, record);
  	}
}
