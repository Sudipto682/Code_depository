package kafka_mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class MySqlFileSinkConnector extends SinkConnector {
	
	public static final String FILE_CONFIG="file";
	 private static final ConfigDef CONFIG_DEF = new ConfigDef()
     .define(FILE_CONFIG, Type.STRING, Importance.HIGH, "sqlRecords.txt");

	 
	 String filename;
	 
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		// TODO Auto-generated method stub
	
		filename=props.get(FILE_CONFIG);
	}

	@Override
	public Class<? extends Task> taskClass() {
		// TODO Auto-generated method stub
		return (Class<? extends Task>) MySqlFileSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// TODO Auto-generated method stub
		ArrayList<Map<String,String>> configs=new ArrayList<Map<String,String>>();

		Map<String,String> config=new HashMap<String, String>();
		
		config.put(FILE_CONFIG, filename);
		configs.add(config);
		return configs;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
