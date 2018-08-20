package vegoo.stockdata.crawler.core;

import java.io.IOException;

public class HttpRequestException extends IOException {

	private int responseCode;

	public HttpRequestException() {
		// TODO Auto-generated constructor stub
	}

	public HttpRequestException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public HttpRequestException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public HttpRequestException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}
	
	public HttpRequestException(int responseCode) {
		this.setResponseCode(responseCode);
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

}
