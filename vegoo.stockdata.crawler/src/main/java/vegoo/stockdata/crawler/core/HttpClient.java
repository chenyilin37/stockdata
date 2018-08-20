package vegoo.stockdata.crawler.core;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HttpClient {
    private final OkHttpClient okHttpClient;
     
    private HttpClient(){
        this.okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    public static HttpClient newInstance() {
    	return new HttpClient();
    }

    public void asyncRequest(List<String> urls, Callback callback) {
        urls.forEach(url -> {
        	asyncRequest(url, callback);
        });
    }
    
    public void asyncRequest(String url, Callback callback) {
    	okHttpClient.newCall(new Request.Builder()
                .url(url)
                .build())
                .enqueue(callback);
    }

    public String syncRequest(String url) throws HttpRequestException,IOException {
        Request request = new Request.Builder()
                .url(url) // This URL is served with a 2 second delay.
                .build();

        Response response = okHttpClient.newCall(request).execute();
        
        if(response.isSuccessful()) {
            return response.body().string();
        }
        
        throw new HttpRequestException(response.code());
        
    }
    
    public String syncRequest(String url, int tryTimes) throws HttpRequestException,IOException {
        Request request = new Request.Builder()
                .url(url) // This URL is served with a 2 second delay.
                .build();
        
        IOException err = null;
        Response response = null;
        
        for(int i=0; i<tryTimes; ++i) {
	        try {
	           response = okHttpClient.newCall(request).execute();
	           if(response.isSuccessful()) {
	               return response.body().string();
	           }
	        }catch(IOException e) {
	        	err = e;
	        }
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
        }
        
        if(response == null) {
        	if(err != null) {
        		throw err;
        	}else {
        		throw new IOException();
        	}
        }
        
        throw new HttpRequestException(response.code());
        
    }
    
}
