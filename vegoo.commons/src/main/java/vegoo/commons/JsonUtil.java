package vegoo.commons;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.google.gson.*;


public class JsonUtil {
	// 查看eastmoney数据的日期格式
	private static Gson gson = null;
    
	// public static Date ERR_DATE = formatter.parse("2018-07-31T00:00:00");
	
	public static Date parseDate(String value) throws ParseException  {
		try {
			// SimpleDateFormat 　非线程安全, 不要使用static做单例
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			return formatter1.parse(value);
		} catch (ParseException e) {
			try {
				SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
				return formatter2.parse(value);
			} catch (ParseException e1) {
				try {
				  SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				  return formatter3.parse(value);
				}catch(ParseException e2) {
				  SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				  return formatter3.parse(value);
				}
			}
		}
	}
	
	public static String checkDateString(String value) {
		try {
			 parseDate(value);
			 return value;
		} catch (ParseException e) {
			return null;
		}
	}
	
    private static Gson getGson() {
	    	if(gson == null) {
	    	   	GsonBuilder builder = new GsonBuilder();
	    	   	
	    	    builder.registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
	    	        public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
	    	           String value = json.getAsString(); 
	    	        	try {
	    	                return parseDate(value);
	    	            } catch (Exception e) {
	    	            	return null;
	    	            }    	
	    	        }
	    	    });
	    	    
	    	    builder.registerTypeAdapter(double.class, new JsonDeserializer<Double>() {
	    	        public Double deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
	    	        	   try {
	    	                return Double.parseDouble(json.getAsString());
	    	            } catch (Exception e) {
	    	            	return  0.0;
	    	            }    	
	    	        }
	    	    });
	    	    
	    	    builder.registerTypeAdapter(int.class, new JsonDeserializer<Integer>() {
	    	        public Integer deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
	    	        	   try {
	    	                return Integer.parseInt(json.getAsString());
	    	            } catch (Exception e) {
	    	            	return  0;
	    	            }    	
	    	            
	    	        }
	    	    });
	    	    gson = builder.create();    		
	    	  }
	    	  return gson;
    }

    public static <T> T fromJson(String json, Class<T> t) {
    	   return getGson().fromJson(json, t);
    }
    
    public static String toJson(Object object) {
    	   return getGson().toJson(object);
    }
    
}