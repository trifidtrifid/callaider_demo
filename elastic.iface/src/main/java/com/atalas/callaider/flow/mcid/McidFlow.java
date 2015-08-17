package com.atalas.callaider.flow.mcid;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;

public class McidFlow extends StorageIndexedInterface {
	
	public Long x_msisdn;
	public String x_IMSI;
	public String x_CID_SAI_LAI;
	public Long x_HLR;
	public Long x_VLR;
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
	public Integer x_sriOpCode;
		
	public GsmTransaction sri;
	public GsmTransaction psi;
	public List<GsmTransaction> errors;
	
	//information from logs
	public String x_client;
	public Location location;
	public Integer x_locError;
	public String x_serviceResult;
	public Integer x_serviceMode;
	public Boolean x_requestFound;
	
	public String x_requestId;
	public Long x_reqestDelay;
	public String x_lbsReqId;
	
	public static class GsmTransaction {		
		public Map<String, Object> request = new HashMap<>();
		public Map<String, Object> response = new HashMap<>();
	}
}
