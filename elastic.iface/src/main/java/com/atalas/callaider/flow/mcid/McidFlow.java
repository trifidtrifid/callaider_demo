package com.atalas.callaider.flow.mcid;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;

public class McidFlow extends StorageIndexedInterface {
	
	public String _MSISDN;
	public String x_IMSI;
	public String x_CID_SAI_LAI;
	public String HLR;
	public String VLR;
	public Boolean x_sriResponse=false;
	public Boolean x_psiResponse=false;
	public Long x_sriRequestTime;
	public Long x_psiRequestTime;
	public Long x_sriResponseDelay;
	public Long x_psiResponseDelay;
	public String x_result="absent";	
	
	
	public GsmTransaction sriForSm;
	public GsmTransaction sri;
	public GsmTransaction psi;
	public List<GsmTransaction> errors;
	
	public static class GsmTransaction {		
		public Map<String, Object> gsm_map_request = new HashMap<>();
		public Map<String, Object> gsm_map_response = new HashMap<>();
	}			
}
