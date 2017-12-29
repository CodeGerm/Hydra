package com.github.codegerm.hydra.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public final class AvroSchemaUtils {

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

}
