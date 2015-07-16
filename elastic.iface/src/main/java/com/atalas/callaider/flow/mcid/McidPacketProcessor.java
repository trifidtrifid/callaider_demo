package com.atalas.callaider.flow.mcid;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;

import com.atalas.callaider.elastic.iface.PacketProcessor;
import com.atalas.callaider.elastic.iface.ProtocolContainer;
import com.atalas.callaider.elastic.iface.storage.StorageInterface;
import com.atalas.callaider.flow.mcid.McidFlow.GsmTransaction;

public class McidPacketProcessor extends PacketProcessor {

	private final StorageInterface si;
	private static class TcapInfo {
		public String tid;
		public int tcapResult;
	}
	
	public McidPacketProcessor(Client client, String indexName) {
		super(client, indexName);
		this.si = new StorageInterface(client, null, indexName);
		//@Delete all Stored objects and initialize field mapping
		this.si.createObjectMapping(McidFlow.class);
	}

	@Override
	public void processNextPacket(List<ProtocolContainer> packet) {
		super.processNextPacket(packet);
		
		TcapInfo currentTcap = null;
		
		
		for (ProtocolContainer proto : packet) {
			String protoName = proto.getDescription().getName();
			if ( "gsm_map".equals(protoName)) {
				String elementType = proto.getTheField("map_component_type");
				if( null!=elementType){
					switch (elementType) {
	
					case "gsm_old.invoke_element":
						processInvoke(proto, currentTcap);
						break;
					case "gsm_old.returnResultLast_element":
						processReturnResultLast(proto, currentTcap);
						break;
					case "gsm_old.returnError_element":
						processReturnError(proto, currentTcap);
						break;
					default:
						logger.debug(" unsupported map element type: " + elementType);
						break;
					}
				} else {
					logger.warn("No field 'map_component_type' in GSM_MAP proto");
				}
			} else if ( "tcap".equals(protoName)) {
				currentTcap = processTcap( proto );
			}
		}
	}

	private TcapInfo processTcap(ProtocolContainer proto) {
		TcapInfo ti = new TcapInfo();
		ti.tid = proto.getTheField("tid");
		//ti.tcapResult = ((Long)proto.getTheField("tcap.result")).intValue();//tcap.result
		return ti;
	}

	void processReturnError(ProtocolContainer proto, TcapInfo currentTcap) {
	}

	void processReturnResultLast(ProtocolContainer proto, TcapInfo currentTcap) {
		int opCode = ((Long)proto.getTheField("opCode")).intValue();
		switch (opCode) {
		case 22:
		case 45:
			processSriResp(proto, currentTcap);
			break;
		case 70:
			processPsiResp(proto, currentTcap);
		default:
			logger.debug("unsupported map opcode in responce: " + opCode);
			break;
		}
	}
	
	void processInvoke(ProtocolContainer proto, TcapInfo currentTcap) {
		int opCode = ((Long)proto.getTheField("opCode")).intValue();
		switch (opCode) {
		case 22:
		case 45:
			processSriReq(proto, currentTcap);
			break;
		case 70:
			processPsiReq(proto, currentTcap);
		default:
			logger.debug("unsupported map opcode in request: " + opCode);
			break;
		}
	}

	void processPsiResp(ProtocolContainer proto, TcapInfo currentTcap){
	
		String tid = currentTcap.tid;
		String tidKey = "psi.m_request.tid";
		
		McidFlow mcidFlow = getFlowByTid(tid, tidKey);
		McidFlow.GsmTransaction gsmTransaction;
		if( null == mcidFlow ){
			mcidFlow = new McidFlow();
			mcidFlow.psi = gsmTransaction = new GsmTransaction();			
		} else {
			gsmTransaction = mcidFlow.psi;
			if(gsmTransaction == null){
				logger.warn("can't find SRI transaction by tid: " + tid);
				mcidFlow.psi = gsmTransaction = new GsmTransaction();
			}
		}
		
		
		String laiOrCid = proto.getTheField("CID_LAI_SAI");
		String result = "success";
		if(laiOrCid.isEmpty())
			result = "fault";
		gsmTransaction.m_response.put("result", result);
		gsmTransaction.m_response.put("CID_LAI_SAI", laiOrCid);
		gsmTransaction.m_response.put("tid", tid);			
		
		si.saveObject(mcidFlow);
	}


	void processPsiReq(ProtocolContainer proto, TcapInfo currentTcap) {
		
		//todo get McidFlow by "SRI.m_response.imsi" 
		String imsi = proto.getTheField("imsi");
		if( null!=imsi){
			Map<String,Object> filter = new HashMap<String, Object>();
			filter.put("sri.m_response.imsi", imsi);
			McidFlow mcidFlow = si.searchSingleObjects( McidFlow.class, filter);

			if(mcidFlow == null){
				logger.warn("can't find PSI req by imsi: " + imsi);
				return;
			}
			
			McidFlow.GsmTransaction gsmTransaction = new McidFlow.GsmTransaction();
			String tid = currentTcap.tid;
			if(tid.isEmpty()){
				logger.warn("SRI request have no tid");
				return;
			}
			
			gsmTransaction.m_request.put("tid", tid);
			mcidFlow.psi = gsmTransaction;
			
			si.saveObject(mcidFlow);
		} else {
			logger.warn("There is NO imsi in PSI.request");
		}
	}

	void processSriResp(ProtocolContainer proto, TcapInfo currentTcap){
		
		String tid = currentTcap.tid;
		String tidKey = "sri.m_request.tid";
		String imsi = proto.getTheField("imsi");
		
		McidFlow mcidFlow = getFlowByTid(tid, tidKey);
		McidFlow.GsmTransaction gsmTransaction;
		if( null != mcidFlow ){			
			gsmTransaction = mcidFlow.sri;
			if(gsmTransaction == null){
				logger.warn("can't find SRI transaction by tid: " + tid);	
				mcidFlow.sri = gsmTransaction = new GsmTransaction();
			}
		} else {
			mcidFlow = new McidFlow();
			mcidFlow.sri = gsmTransaction = new GsmTransaction();			
		}
		
		//@TODO it's a paranoia to check below!
		gsmTransaction = mcidFlow.sri;
		if(gsmTransaction == null){
			logger.warn("can't find SRI transaction by tid: " + tid);
			return;
		}
		imsi = proto.getTheField("imsi");
		
		gsmTransaction.m_response.put("imsi", imsi);
		gsmTransaction.m_response.put("tid", tid);
		
		si.saveObject(mcidFlow);		

	}

	private McidFlow getFlowByTid(String tid, String tidKey) {
		McidFlow mcidFlow = null;
		
		if( null==tid)
			logger.error("tid = NULL!");
		else {
			Map<String,Object> filter = new HashMap<>();		
			filter.put(tidKey, tid);
			List<McidFlow> mcidFlowL =  si.searchObjects(McidFlow.class, filter);
			
			if(mcidFlowL.size() == 0){
				logger.warn("can't find SRI req by tid: " + tid);
				return null;
			} else if(mcidFlowL.size() > 1){
				logger.warn("There are "+ mcidFlowL.size() +" MCid flows found by filter sri.m_request.tid="+tid);
			}	
			mcidFlow = mcidFlowL.get(0);
		}
		return mcidFlow;
	}
	
	void processSriReq(ProtocolContainer proto, TcapInfo currentTcap) {
		McidFlow mcidFlow = new McidFlow();
		McidFlow.GsmTransaction sriReq = new McidFlow.GsmTransaction();
		String tid = currentTcap.tid;
		if(tid.isEmpty()){
			logger.warn("SRI request have no tid");
			return;
		}
		sriReq.m_request.put("tid", tid);
		mcidFlow.sri = sriReq;		

		si.saveObject(mcidFlow);		
	}

	static Logger logger = Logger.getLogger(McidPacketProcessor.class.getName());

}
