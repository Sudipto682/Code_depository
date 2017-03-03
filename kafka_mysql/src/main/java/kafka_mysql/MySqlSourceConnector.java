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
import org.apache.kafka.connect.source.SourceConnector;

public class MySqlSourceConnector extends SourceConnector {

	public static final String TOPIC_CONFIG="topic";
	public static final String CONNECTION_CONFIG="connection.url";
	public static final String USER_CONFIG="user";
	public static final String PASSWORD_CONFIG="password";
	public static final String DATABASE_CONFIG="database";
	
	
	 private static final ConfigDef CONFIG_DEF = new ConfigDef()
     .define(CONNECTION_CONFIG, Type.STRING, Importance.HIGH, "com.mysql.jdbc.Driver/jdbc:mysql://localhost:3306")
     .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "mysqlTopic")
     .define(USER_CONFIG, Type.STRING, Importance.HIGH, "root")
     .define(PASSWORD_CONFIG, Type.STRING, Importance.HIGH, "wsxedc")
     .define(DATABASE_CONFIG, Type.STRING, Importance.HIGH,"kafka");
	 
	 String connection;
	 String topic;
	 String user;
	 String password;
	 String database;
	
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		// TODO Auto-generated method stub
		connection=props.get(CONNECTION_CONFIG);
		topic=props.get(TOPIC_CONFIG);
		user=props.get(USER_CONFIG);
		password=props.get(PASSWORD_CONFIG);
		database=props.get(DATABASE_CONFIG);
		
		
	}

	@Override
	public Class<? extends Task> taskClass() {
		// TODO Auto-generated method stub
		return (Class<? extends Task>) MySqlSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		// TODO Auto-generated method stub
		ArrayList<Map<String,String>> configs=new ArrayList<Map<String, String>>();
		Map<String,String> config=new HashMap<String, String>();
		config.put(DATABASE_CONFIG, database);
		config.put(PASSWORD_CONFIG, password);
		config.put(TOPIC_CONFIG, topic);
		config.put(USER_CONFIG, user);
		config.put(CONNECTION_CONFIG, connection);
		
		configs.add(config);
		return configs;
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}
	
	public ConfigDef config() {
        return CONFIG_DEF;
    }


}
