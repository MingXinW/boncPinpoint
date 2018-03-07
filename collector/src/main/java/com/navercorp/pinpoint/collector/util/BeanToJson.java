package com.navercorp.pinpoint.collector.util;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BeanToJson {
	
	public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	public static <T> JSONObject toEsTime(T t) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String text = mapper.writeValueAsString(t);
		JSONObject jsonbject = JSONObject.parseObject(text);
		Calendar calendar = Calendar.getInstance();
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		String timestamp = df.format(calendar.getTime());
		jsonbject.put("@timestamp", timestamp);
		return jsonbject;
	}
	
	public static JSONObject addEsTime(JSONObject jsonbject) {
		if(jsonbject != null) {
			Calendar calendar = Calendar.getInstance();
			df.setTimeZone(TimeZone.getTimeZone("UTC"));
			String timestamp = df.format(calendar.getTime());
			jsonbject.put("@timestamp", timestamp);
		}
		return jsonbject;
	}
}
