
package org.apache.zeppelin.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

/**
 * "Experimental" MongoDB Interpreter for Zeppelin
 */
public class MongoDBInterpreter extends Interpreter {

private static Logger logger = LoggerFactory.getLogger(MongoDBInterpreter.class);

  private static final String HELP = "MongoDB interpreter:\n"
    + "General format: <command> /<db>/<collection> <option> <JSON>\n"
    + "  - find /db/collection/id\n";

  private static final List<String> COMMANDS = Arrays.asList(
    "find");
    

  static {
	    Interpreter.register("mongodb", MongoDBInterpreter.class.getName());
	  }
 
  private MongoClient client;
  private DB database;
  private DBCollection collection;
  private String host = "localhost";
  private int port = 27017;

  public MongoDBInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
	  client = new MongoClient(host,port);
  }

	  
  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    logger.info("Run MongoDB command '" + cmd + "'");
 
    if (StringUtils.isEmpty(cmd) || StringUtils.isEmpty(cmd.trim())) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    }

    if (client == null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
        "Problem with the MongoDB client, please check your configuration (host, port,...)");
    }

    String[] items = StringUtils.split(cmd.trim(), " ", 3);

    if ("help".equalsIgnoreCase(items[0])) {
      return processHelp(InterpreterResult.Code.SUCCESS, null);
    }
 
    if (items.length < 2) {
      return processHelp(InterpreterResult.Code.ERROR, "Arguments missing");
    }

    final String method = items[0];
    final String url = items[1];
    final String data = items.length > 2 ? items[2].trim() : null;

    final String[] urlItems = StringUtils.split(url.trim(), "/");

    try {
      if ("find".equalsIgnoreCase(method)) {
        return processFind(urlItems,data);
      }
      return processHelp(InterpreterResult.Code.ERROR, "Unknown command");
    }
    catch (Exception e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
    }
  }


  @Override
  public int getProgress(InterpreterContext interpreterContext) {
    return 0;
  }

  @Override
  public List<String> completion(String s, int i) {
    final List<String> suggestions = new ArrayList<>();

    if (StringUtils.isEmpty(s)) {
      suggestions.addAll(COMMANDS);
    }
    else {
      for (String cmd : COMMANDS) {
        if (cmd.toLowerCase().contains(s)) {
          suggestions.add(cmd);
        }
      }
    }

    return suggestions;
  }

  private InterpreterResult processHelp(InterpreterResult.Code code, String additionalMessage) {
    final StringBuffer buffer = new StringBuffer();

    if (additionalMessage != null) {
      buffer.append(additionalMessage).append("\n");
    }

    buffer.append(HELP).append("\n");

    return new InterpreterResult(code, InterpreterResult.Type.TEXT, buffer.toString());
  }


  private InterpreterResult processFind(String[] urlItems,String data) {

    if (urlItems.length != 2 
        || StringUtils.isEmpty(urlItems[0]) 
        || StringUtils.isEmpty(urlItems[1])) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
                                   "Bad URL (it should be /db/collection)");
    }
    
    DBObject query = (DBObject) JSON.parse(data);
    
    database = client.getDB(urlItems[0]);
    collection = database.getCollection(urlItems[1]);
    List<DBObject> obj = collection.find(query).toArray();
    
    String json = "";
    
    if(obj.size() > 0) {
    	StringBuffer row = new StringBuffer();
    	for(int i=0; i < obj.size(); i++){
    		if(i==0){
    			row.append(formTable(obj.get(i).toString(),true));
    		}else {
    			row.append(formTable(obj.get(i).toString(),false));
    		}
    		json = row.toString();	
    	}
      
    	return (new InterpreterResult(
                    InterpreterResult.Code.SUCCESS,
                    InterpreterResult.Type.TABLE,
                    json)); 
    	
    }        
        
    return new InterpreterResult(InterpreterResult.Code.ERROR, "Document not found");
  }
  
private String formTable(String json, boolean includeHeader){
	StringBuffer ret = new StringBuffer();
	
	Map<String, Object> retMap = new Gson().fromJson(json, new TypeToken<HashMap<String, Object>>() {}.getType());
	
	if(includeHeader){
		for (Entry<String, Object> entry : retMap.entrySet()) {
			ret = ret.append(entry.getKey()).append("\t");
		}
		ret.append("\n");
	}
	for (Entry<String, Object> entry : retMap.entrySet()) {
		ret.append(entry.getValue()).append("\t");
	}
	ret.append("\n");
	return ret.toString();
}

@Override
public void cancel(InterpreterContext arg0) {
	// TODO Auto-generated method stub
	
}

@Override
public FormType getFormType() {
	
	return FormType.SIMPLE;
}
}

