package com.atalas.callaider.flow.mcid;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;

public class McidFlow extends StorageIndexedInterface {
	
	public String x_msisdn;
	public String x_IMSI;
	public String x_CID_SAI_LAI;
	public String x_HLR;
	public String x_VLR;
	public Boolean x_sriResponse=false;
	public Boolean x_psiResponse=false;
	public Long x_sriRequestTime;
	public Long x_psiRequestTime;
	public Long x_sriResponseDelay;
	public Long x_psiResponseDelay;
	public String x_result="absent";
	public Integer x_sriErrorCode = 0;
	public Integer x_psiErrorCode = 0;
	public Boolean x_pagingFlag = false;		
		
	public GsmTransaction sriForSm;
	public GsmTransaction sri;
	public GsmTransaction psi;
	public List<GsmTransaction> errors;
	
	public String x_client;
	
	public static class GsmTransaction {		
		public Map<String, Object> gsm_map_request = new HashMap<>();
		public Map<String, Object> gsm_map_response = new HashMap<>();
	}		
}
