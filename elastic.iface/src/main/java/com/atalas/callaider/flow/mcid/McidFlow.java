package com.atalas.callaider.flow.mcid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;

public class McidFlow extends StorageIndexedInterface {
	
	public GsmTransaction sriForSm;
	public GsmTransaction sri;
	public GsmTransaction psi;
	public List<GsmTransaction> errors;
	
	public static class GsmTransaction {		
		public Map<String, Object> m_request = new HashMap<>();
		public Map<String, Object> m_response = new HashMap<>();
	}			
}
