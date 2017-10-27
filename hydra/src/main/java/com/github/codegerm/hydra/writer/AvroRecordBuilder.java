package com.github.codegerm.hydra.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class AvroRecordBuilder {

	public static byte[] serialize(List<Object> result, String entitySchema) throws IOException{
		if(result == null)
			return null;
		Schema schema = new Schema.Parser().parse(entitySchema);
		if(schema.getFields().size()!=result.size())
			throw new IllegalStateException("Schema size is not same as result size");

		GenericRecord record = new GenericData.Record(schema);


		for(int i=0;i<result.size();i++){
			Object obj = result.get(i);
			record.put(i, convert(obj));
		}

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		BinaryEncoder encoder = null;
		encoder = EncoderFactory.get().binaryEncoder(stream, encoder);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		datumWriter.write(record, encoder);
		encoder.flush();
		return stream.toByteArray();


	}


	public static GenericRecord deserialize(byte[] data, String entitySchema) throws IOException{
		if(data == null)
			return null;
		Schema schema = new Schema.Parser().parse(entitySchema);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
		GenericRecord record = datumReader.read(null, decoder);
		return record;
	}

	private static Object convert(Object obj){
		return obj;
	}



}
