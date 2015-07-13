package com.atalas.callaider.elastic.iface;
import java.util.Map;

public class McidFlow {

	public static class GsmTransaction {
		public Map<String, Object> m_request;
		public Map<String, Object> m_response;
	}

	public Map<String, GsmTransaction> m_transactions;
		
	
}
