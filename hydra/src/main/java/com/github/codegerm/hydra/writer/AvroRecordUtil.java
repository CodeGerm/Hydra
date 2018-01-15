package com.github.codegerm.hydra.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroRecordUtil {

	private static final Logger LOG = LoggerFactory.getLogger(AvroRecordUtil.class);

	private static BinaryEncoder encoder;
	private static BinaryDecoder binaryDecoder;

	public static byte[] serializeToBinary(List<Object> result, String entitySchema) throws IOException {
		GenericRecord record = serialize(result, entitySchema);
		ByteArrayOutputStream stream = null;
		try {
			stream = new ByteArrayOutputStream();
			encoder = EncoderFactory.get().binaryEncoder(stream, encoder);
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(record.getSchema());
			datumWriter.write(record, encoder);
			encoder.flush();
			return stream.toByteArray();
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	public static byte[] serializeToJson(List<Object> result, String entitySchema) throws IOException {
		GenericRecord record = serialize(result, entitySchema);
		ByteArrayOutputStream stream = null;
		try {
			stream = new ByteArrayOutputStream();
			JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), stream);
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(record.getSchema());
			datumWriter.write(record, encoder);
			encoder.flush();
			return stream.toByteArray();
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	public static GenericRecord serialize(List<Object> result, String entitySchema) throws IOException {
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
			record.put(i, convertFromSqlToAvro(obj));
		}
		return record;
	}

	public static GenericRecord deserializeFromBinary(byte[] data, String entitySchema) throws IOException {
		if (data == null) {
			return null;
		}
		Schema schema = new Schema.Parser().parse(entitySchema);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		binaryDecoder = DecoderFactory.get().binaryDecoder(data, binaryDecoder);
		GenericRecord record = datumReader.read(null, binaryDecoder);
		return record;
	}

	public static GenericRecord deserializeFromJson(String data, String entitySchema) throws IOException {
		if (data == null) {
			return null;
		}
		Schema schema = new Schema.Parser().parse(entitySchema);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema, data);
		GenericRecord record = datumReader.read(null, decoder);
		return record;
	}

	public static GenericRecord deserializeFromJson(byte[] data, String entitySchema) throws IOException {
		return deserializeFromJson(new String(data, StandardCharsets.UTF_8), entitySchema);
	}

	public static String getSchemaName(String entitySchema) {
		Schema schema = new Schema.Parser().parse(entitySchema);
		return schema.getName();
	}

	public static Object convertFromSqlToAvro(Object obj) {
		if (obj == null) {
			return null;
		}
		if (obj instanceof java.sql.Timestamp) {
			return ((Timestamp) obj).getTime();
		}
		if (obj instanceof java.sql.Clob) {
			Reader reader = null;
			try {
				Clob clob = (Clob) obj;
				reader = clob.getCharacterStream();
				return IOUtils.toString(reader);
			} catch (SQLException | IOException e) {
				LOG.warn("Get clob value failed", e);
				return null;
			} finally {
				IOUtils.closeQuietly(reader);
			}
		}
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
