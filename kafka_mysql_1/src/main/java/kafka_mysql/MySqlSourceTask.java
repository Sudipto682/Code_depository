package kafka_mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

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
	 
	/* Schema schema=SchemaBuilder.struct().name("test_Schema")
			 					.field("id", Schema.INT64_SCHEMA)
			 					.field("name", Schema.STRING_SCHEMA)
			 					.field("marks", Schema.INT32_SCHEMA)
			 					.build();
*/
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
			
			Map<String,Object> offset=context.offsetStorageReader().offset(Collections.singletonMap(DATABASE,database));
			Object offset_time_o=offset.get(TIME);
			Long offset_time=(Long) offset_time_o;
			//ResultSet myts=myst.executeQuery("select * from kafka.test where time>offset_time");
			//Long ts=myts.getLong("time");
			
			
			ResultSet myrs=myst.executeQuery("Select * from kafka.test  where time>offset_time");
			while(myrs.next())
			{
				System.out.println(myrs.getString("name")+ ","+ myrs.getInt("id")+ ","+ myrs.getInt("marks"));
				if(records==null)
				records=new ArrayList<SourceRecord>();
				
				//generating timestamp for mysql records
				java.util.Date today=new java.util.Date();
				java.sql.Timestamp ts=new java.sql.Timestamp(today.getTime());
				Long tsTime=ts.getTime();
				System.out.println(tsTime);
				
				
				
				
				//coverting processed records to json string
				
				Schema_Mysql_database obj=new Schema_Mysql_database();
				obj.setId(myrs.getInt("id"));
				obj.setName(myrs.getString("name"));
				obj.setMarks(myrs.getInt("marks"));
				obj.setTime(myrs.getLong("time"));
				
				//Long tsTime=myrs.getLong("time");
				
				ObjectMapper mapper = new ObjectMapper();
				String json = mapper.writeValueAsString(obj);
				
			
				
				
				
				
				
				
				records.add(new SourceRecord(Collections.singletonMap(DATABASE, database), Collections.singletonMap(TIME,tsTime), topic, Schema.STRING_SCHEMA,json));

			};
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
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
