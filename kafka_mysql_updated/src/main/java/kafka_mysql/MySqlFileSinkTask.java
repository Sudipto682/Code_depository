package kafka_mysql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;








import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class MySqlFileSinkTask extends SinkTask{

	
	public String filename;
	private PrintStream outputstream;
	ObjectMapper mapper = null;
	
	public String version() {
		// TODO Auto-generated method stub
		return new  MySqlFileSinkConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		// TODO Auto-generated method stub
		filename=props.get(MySqlFileSinkConnector.FILE_CONFIG);
		 mapper = new ObjectMapper();
		 
		 try {
			  mapper = new ObjectMapper();
			 File file=new File(filename);
			outputstream=new PrintStream(new FileOutputStream(filename,true));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		// TODO Auto-generated method stub
		
		
		for(SinkRecord rec:records)
		{
			String json=(String) rec.value();
		
			try {
				Schema_Mysql_database obj = mapper.readValue(json, Schema_Mysql_database.class);
				System.out.println(obj.getId()+"  "+ obj.getName()+"   "+  obj.getMarks());
				outputstream.println(obj.getId()+"  "+ obj.getName()+"   "+  obj.getMarks());
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		// TODO Auto-generated method stub
		outputstream.flush();
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
