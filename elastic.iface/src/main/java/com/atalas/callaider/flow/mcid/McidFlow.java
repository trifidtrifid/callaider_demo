package com.atalas.callaider.flow.mcid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;

public class McidFlow extends StorageIndexedInterface {
	
	public String MSISDN;
	public String IMSI;
	public String CID_SAI_LAI;
	public String HLR;
	public String VLR;
	public Boolean sriResponse;
	public Boolean psiResponse;
	public Long sriRequestTime;
	public Long psiRequestTime;
	public Long sriResponseDelay;
	public Long psiResponseDelay;
	public String result;	
	
	public GsmTransaction sriForSm;
	public GsmTransaction sri;
	public GsmTransaction psi;
	public List<GsmTransaction> errors;
	
	public static class GsmTransaction {		
		public Map<String, Object> gsm_map_request = new HashMap<>();
		public Map<String, Object> gsm_map_response = new HashMap<>();
	}			
}
