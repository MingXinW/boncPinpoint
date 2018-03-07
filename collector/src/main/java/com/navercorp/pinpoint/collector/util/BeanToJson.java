package com.navercorp.pinpoint.collector.util;


import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BeanToJson {

	public static <T> JSONObject toEsTime(T t) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String text = mapper.writeValueAsString(t);
		JSONObject jsonbject = JSONObject.parseObject(text);
		jsonbject.put("@timestamp", Long.toString(System.currentTimeMillis()));
		return jsonbject;
	}
	
	public static JSONObject addEsTime(JSONObject jsonbject) {
		if(jsonbject != null) {
			jsonbject.put("@timestamp", Long.toString(System.currentTimeMillis()));
		}
		return jsonbject;
	}
	
}
