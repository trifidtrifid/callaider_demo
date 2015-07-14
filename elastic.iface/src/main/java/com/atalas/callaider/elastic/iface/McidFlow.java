package com.atalas.callaider.elastic.iface;
import java.util.Map;
import java.util.List;;

public class McidFlow {

	public String id;
	
	public static class GsmTransaction {
		Map<String, Object> m_request;
		Map<String, Object> m_response;
	}

	List<GsmTransaction> m_transactions;
		
	
}
