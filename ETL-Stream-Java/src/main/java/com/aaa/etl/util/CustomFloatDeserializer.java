package com.aaa.etl.util;

import java.io.IOException;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class CustomFloatDeserializer extends JsonDeserializer<Float> {

	@Override
	public Float deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
		// TODO Auto-generated method stub
		String floatString = p.getText();
		if (floatString.equals(".")) {
			return null;
		}
		
		return Float.valueOf(floatString);
	}

}
