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
	
	public McidPacketProcessor(Client client, String indexName) {
		super(client, indexName);
		this.si = new StorageInterface(client, null, indexName);
		//@Delete all Stored objects and initialize field mapping
		this.si.createObjectMapping(McidFlow.class);
	}

	@Override
	public void processNextPacket(List<ProtocolContainer> packet) {
		super.processNextPacket(packet);

		for (ProtocolContainer proto : packet) {
			if ( "gsm_map".equals(proto.getDescription().getName())) {
				String elementType = proto.getTheField("map_component_type");
				if( null!=elementType){
					switch (elementType) {
	
					case "gsm_old.invoke_element":
						processInvoke(proto);
						break;
					case "gsm_old.returnResultLast_element":
						processReturnResultLast(proto);
						break;
					case "gsm_old.returnError_element":
						processReturnError(proto);
						break;
					default:
						logger.debug(" unsupported map element type: " + elementType);
						break;
					}
				} else {
					logger.warn("No field 'map_component_type' in GSM_MAP proto");
				}
			}
		}
	}

	void processReturnError(ProtocolContainer proto) {
	}

	void processReturnResultLast(ProtocolContainer proto) {
		int opCode = ((Long)proto.getTheField("opCode")).intValue();
		switch (opCode) {
		case 22:
		case 45:
			processSriResp(proto);
			break;
		case 70:
			processPsiResp(proto);
		default:
			logger.debug("unsupported map opcode in responce: " + opCode);
			break;
		}
	}
	
	void processInvoke(ProtocolContainer proto) {
		int opCode = ((Long)proto.getTheField("opCode")).intValue();
		switch (opCode) {
		case 22:
		case 45:
			processSriReq(proto);
			break;
		case 70:
			processPsiReq(proto);
		default:
			logger.debug("unsupported map opcode in request: " + opCode);
			break;
		}
	}

	void processPsiResp(ProtocolContainer proto){
	
		String tid = proto.getTheField("tid");
		String tidKey = "psi.m_request.tid";
		
		McidFlow mcidFlow = getFlowByTid(tid, tidKey);
		if( null != mcidFlow ){
		
			McidFlow.GsmTransaction gsmTransaction = mcidFlow.psi;
			if(gsmTransaction == null){
				logger.warn("can't find SRI transaction by tid: " + tid);
				return;
			}
			String laiOrCid = proto.getTheField("CID_LAI_SAI");
			String result = "success";
			if(laiOrCid.isEmpty())
				result = "fault";
			gsmTransaction.m_response.put("result", result);
		}			
	}


	void processPsiReq(ProtocolContainer proto) {
		
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
			String tid = proto.getTheField("tid");
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

	void processSriResp(ProtocolContainer proto){
		
		String tid = proto.getTheField("tid");
		String tidKey = "sri.m_request.tid";
		
		McidFlow mcidFlow = getFlowByTid(tid, tidKey);
		if( null != mcidFlow ){
		
			//@TODO it's a paranoia to check below!
			McidFlow.GsmTransaction gsmTransaction = mcidFlow.sri;
			if(gsmTransaction == null){
				logger.warn("can't find SRI transaction by tid: " + tid);
				return;
			}
			String imsi = proto.getTheField("imsi");
			
			gsmTransaction.m_response.put("imsi", imsi);
			gsmTransaction.m_response.put("tid", tid);
			
			si.saveObject(mcidFlow);		
		}
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
	
	void processSriReq(ProtocolContainer proto) {
		McidFlow mcidFlow = new McidFlow();
		McidFlow.GsmTransaction sriReq = new McidFlow.GsmTransaction();
		String tid = proto.getTheField("tid");
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
