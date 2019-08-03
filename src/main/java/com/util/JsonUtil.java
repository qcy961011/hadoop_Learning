package com.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
	private static final ObjectMapper mapper = new ObjectMapper();

	public static final String toJson(Object input) {
		ByteArrayOutputStream bio = new ByteArrayOutputStream();
		String result = "";
		try {
			result = mapper.writeValueAsString(input);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bio.close();
			} catch (IOException e) {
			}
		}
		return result;
	}

	public static final <T> T fromJson(String json, Class<T> clazz) {
		Object result = null;
			try {
                result = mapper.readValue(json, clazz);
            } catch (JsonParseException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
		return (T) result;
	}

	/**
	 * 获取泛型的Collection Type
	 * 
	 * @param jsonStr
	 *            json字符串
	 * @param collectionClass
	 *            泛型的Collection
	 * @param elementClasses
	 *            元素类型
	 */
	public static <T> T readJson(String jsonStr, Class<?> collectionClass,
			Class<?>... elementClasses) throws Exception {
		Object result = null;
		ObjectMapper mapper = new ObjectMapper();

		JavaType javaType = mapper.getTypeFactory().constructParametricType(
				collectionClass, elementClasses);

		result= mapper.readValue(jsonStr, javaType);
		 return (T)result;

	}

	public static Map readJson(String jsonStr) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		Map readValue = mapper.readValue(jsonStr, Map.class);
		return readValue;
	}



	public static ObjectMapper getMapper() {
		return mapper;
	}

}
