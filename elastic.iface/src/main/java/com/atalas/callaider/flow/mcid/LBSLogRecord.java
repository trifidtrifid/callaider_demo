package com.atalas.callaider.flow.mcid;

import java.util.Date;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;
import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface.Location;

public class LBSLogRecord extends StorageIndexedInterface {
	public String reqId;			
	public Long msisdn;
	public Integer mode;	
	public String uid;
	public String result;
	public Location location;
	public Integer err;
	public Integer age;	
	public Boolean flowFound = false;
	public String flowId;
}