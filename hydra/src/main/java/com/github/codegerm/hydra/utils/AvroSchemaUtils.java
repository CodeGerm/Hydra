package com.github.codegerm.hydra.utils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.codegerm.hydra.source.SqlSourceUtil;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public final class AvroSchemaUtils {

	private static final Logger logger = LoggerFactory.getLogger(AvroSchemaUtils.class);

	public static List<Schema> getSchemasAsList(String schemas) {
		if (Strings.isNullOrEmpty(schemas)) {
			return null;
		}
		try {
			List<Schema> schemaList = new ArrayList<>();
			JsonElement root = new JsonParser().parse(schemas);
			if (root.isJsonArray()) {
				JsonArray array = root.getAsJsonArray();
				for (int i = 0; i < array.size(); i++) {
					String jsonStr = array.get(i).toString();
					Schema schema = new Schema.Parser().parse(jsonStr);
					schemaList.add(schema);
				}
			} else if (root.isJsonObject()) {
				JsonObject obj = root.getAsJsonObject();
				String jsonStr = obj.toString();
				Schema schema = new Schema.Parser().parse(jsonStr);
				schemaList.add(schema);
			}
			return schemaList;
		} catch (Exception e) {
			logger.warn("Schema parsing failed", e);
			return null;
		}
	}

	public static Map<String, String> getSchemasAsStringMap(String schemas) {
		List<Schema> schemaList = getSchemasAsList(schemas);
		if (schemaList == null) {
			return null;
		}
		Map<String, String> schemaMap = new HashMap<>();
		for (Schema schema : schemaList) {
			String key = schema.getFullName();
			schemaMap.put(key, schema.toString());
		}
		return schemaMap;
	}

	/*
	 *  { "schema1.table1" : "schema2.table1", "table1" : "table2" }
	 */
	public static Map<String, String> replaceTableNameByEnv(Map<String, String> schemaMap) {
		String mapString = System.getenv(SqlSourceUtil.TABLE_NAME_REPLACE_ENV);	
		
		try{
			if(mapString != null && !mapString.isEmpty()){
				logger.info("Replace table in env found: " + mapString);
				Type mapType = new TypeToken<Map<String, String>>(){}.getType();  
				Map<String, String> map = new Gson().fromJson(mapString, mapType);
				Map<String, String> nodeMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
				nodeMap.putAll(map);
				Map<String, String> newSchemaMap = new HashMap<String, String>();
				for(String k : schemaMap.keySet()){
					if(nodeMap.containsKey(k)){
						String newName = nodeMap.get(k);
						logger.info("Replace table: " + k +" to " + newName);
						newSchemaMap.put(newName, schemaMap.get(k));
					} else
						newSchemaMap.put(k, schemaMap.get(k));
				}
				return newSchemaMap;
			} else {
				logger.info("No replace table in env found: ");
			}
		} catch (Exception e){
			logger.error("Table name replace failed, keep orginal name", e);
		}

		return schemaMap;

	}

}
