package kafka_mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class MySqlSourceTask  extends SourceTask{
	
	String connection;
	 String topic;
	 String user;
	 String password;
	 String database;
	 
	 public static final String DATABASE="database";
	 public static final String TIME="time";
	 
	 Connection myconn=null;
	 
	 //schema
	 
	 Schema schema=SchemaBuilder.struct().name("test_Schema")
			 					.field("id", Schema.INT64_SCHEMA)
			 					.field("name", Schema.STRING_SCHEMA)
			 					.field("marks", Schema.INT32_SCHEMA)
			 					.build();

	public String version() {
		// TODO Auto-generated method stub
		return new MySqlSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		// TODO Auto-generated method stub
		
		
		connection=props.get(MySqlSourceConnector.CONNECTION_CONFIG);
		topic=props.get(MySqlSourceConnector.TOPIC_CONFIG);
		user=props.get(MySqlSourceConnector.USER_CONFIG);
		password=props.get(MySqlSourceConnector.PASSWORD_CONFIG);
		database=props.get(MySqlSourceConnector.DATABASE_CONFIG);
		
		//opening the connection for jdbc mysql
		
		try {
			 myconn=DriverManager.getConnection(connection, user, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<SourceRecord> records=null;
		try {
			Statement myst=myconn.createStatement();
			ResultSet myrs=myst.executeQuery("Select * from kafka.test");
			while(myrs.next())
			{
				System.out.println(myrs.getString("name")+ ","+ myrs.getInt("id")+ ","+ myrs.getInt("marks"));
				records=new ArrayList<SourceRecord>();
				java.util.Date today=new java.util.Date();
				java.sql.Timestamp ts=new java.sql.Timestamp(today.getTime());
				Long tsTime=ts.getTime();
				System.out.println(tsTime);
				records.add(new SourceRecord(Collections.singletonMap(DATABASE, database), Collections.singletonMap(TIME,tsTime), topic, schema, myrs));

			};
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return records;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
