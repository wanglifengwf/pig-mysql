package com.tsl.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MysqlRecordWriter extends RecordWriter<Text, MysqlRecord> {
  	private static final Log LOG = LogFactory.getLog(MysqlRecordWriter.class);
	
	Relation vTable = null;
	String schemaName = null;
	Connection connection = null;
	PreparedStatement statement = null;
	long batchSize = 0;
	long numRecords = 0;

	public MysqlRecordWriter(Connection conn, String sql, long batch)
		throws SQLException 
	{
		this.connection = conn;
		batchSize = batch;
		LOG.info("batchSize " + batchSize);
		statement = conn.prepareStatement(sql);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException {
	    try {
            LOG.info("executeBatch called during close");
            statement.executeBatch();
        } catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void write(Text table, MysqlRecord record) throws IOException {
		if (table != null && !table.toString().equals(vTable.getTable()))
			throw new IOException("Writing to different table " + table.toString()
					+ ". Expecting " + vTable.getTable());

		try {
			record.write(statement);
			numRecords++;
			if (numRecords % batchSize == 0) {
                LOG.info("executeBatch called on batch of size " + numRecords + " " + batchSize);
				statement.executeBatch();
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
