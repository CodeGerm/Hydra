package com.github.codegerm.hydra.writer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;

public class JsonRecordUtil {

	public static byte[] serialize(List<Object> result, String entitySchema) throws IOException {
		if (result == null) {
			return null;
		}

		Schema schema = new Schema.Parser().parse(entitySchema);
		if (schema.getFields().size() != result.size()) {
			throw new IllegalStateException("Schema size is not same as result size");
		}

		GenericRecord record = new GenericData.Record(schema);
		for (int i = 0; i < result.size(); i++) {
			Object obj = result.get(i);
			record.put(i, convert(obj));
		}

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, stream);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		datumWriter.write(record, encoder);
		encoder.flush();
		return stream.toByteArray();

	}

	public static GenericRecord deserialize(byte[] data, String entitySchema) throws IOException {
		if (data == null) {
			return null;
		}
		Schema schema = new Schema.Parser().parse(entitySchema);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		ByteArrayInputStream stream = new ByteArrayInputStream(data);
		JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, stream);
		GenericRecord record = datumReader.read(null, decoder);
		return record;
	}

	public static String getSchemaName(String entitySchema) {
		Schema schema = new Schema.Parser().parse(entitySchema);
		return schema.getName();
	}

	public static Object convert(Object obj) {
		if (obj.getClass().getName().equals("java.sql.Timestamp"))
			return (((Timestamp) obj).getTime());
		return obj;
	}

	public static List<String> getEntityFields(String entitySchema) {
		List<String> fields = new ArrayList<String>();
		Schema schema = new Schema.Parser().parse(entitySchema);
		for (Field field : schema.getFields()) {
			fields.add(field.name());
		}
		return fields;
	}

}
