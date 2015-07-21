package com.atalas.callaider.flow.mcid;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;

import com.atalas.callaider.elastic.iface.PacketProcessor;
import com.atalas.callaider.elastic.iface.ProtocolContainer;
import com.atalas.callaider.elastic.iface.storage.StorageInterface;
import com.atalas.callaider.flow.mcid.McidFlow.GsmTransaction;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;

public class McidPacketProcessor extends PacketProcessor {

	private final StorageInterface si;

	private static class TcapInfo {
		public String tid;
		public int tcapResult;
	}

	private final ConcurrentLinkedHashMap<String, McidFlow> tidMap;
	private final ConcurrentLinkedHashMap<String, McidFlow> imsiMap;

	public McidPacketProcessor(Client client, String indexName) {
		super(client, indexName);
		this.si = new StorageInterface(client, null, indexName);
		// @Delete all Stored objects and initialize field mapping
		this.si.createObjectMapping(McidFlow.class);
		ConcurrentLinkedHashMap.Builder<String, McidFlow> builder = new Builder<>();
		builder.maximumWeightedCapacity(1000000L);
		tidMap = builder.build();
		imsiMap = builder.build();

	}

	@Override
	public void processNextPacket(List<ProtocolContainer> packet) {
		super.processNextPacket(packet);

		TcapInfo currentTcap = null;

		for (ProtocolContainer proto : packet) {
			String protoName = proto.getDescription().getName();
			if ("gsm_map".equals(protoName)) {
				String elementType = proto.getTheField("map_component_type");
				if (null != elementType) {
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
						logger.debug(" unsupported map element type: "
								+ elementType);
						break;
					}
				} else {
					logger.warn("No field 'map_component_type' in GSM_MAP proto");
				}
			} else if ("tcap".equals(protoName)) {
				currentTcap = processTcap(proto);
			}
		}
	}

	private TcapInfo processTcap(ProtocolContainer proto) {
		TcapInfo ti = new TcapInfo();
		ti.tid = proto.getTheField("tid");
		// ti.tcapResult =
		// ((Long)proto.getTheField("tcap.result")).intValue();//tcap.result
		return ti;
	}

	void processReturnError(ProtocolContainer proto, TcapInfo currentTcap) {
	}

	void processReturnResultLast(ProtocolContainer proto, TcapInfo currentTcap) {
		int opCode = ((Long) proto.getTheField("opCode")).intValue();
		switch (opCode) {
		case 22:
		case 45:
			processSriResp(proto, currentTcap);
			break;
		case 70:
			processPsiResp(proto, currentTcap);
			break;
		default:
			logger.debug("unsupported map opcode in responce: " + opCode);
			break;
		}
	}

	void processInvoke(ProtocolContainer proto, TcapInfo currentTcap) {
		int opCode = ((Long) proto.getTheField("opCode")).intValue();
		switch (opCode) {
		case 22:
		case 45:
			processSriReq(proto, currentTcap);
			break;
		case 70:
			processPsiReq(proto, currentTcap);
			break;
		default:
			logger.debug("unsupported map opcode in request: " + opCode);
			break;
		}
	}

	void processPsiResp(ProtocolContainer proto, TcapInfo currentTcap) {

		String tid = currentTcap.tid;
		String tidKey = "psi.gsm_map_request.tid";

		McidFlow mcidFlow = getFlowByTid(tid, tidKey);
		McidFlow.GsmTransaction gsmTransaction;
		if (null == mcidFlow) {
			mcidFlow = new McidFlow();
			mcidFlow.psi = gsmTransaction = new GsmTransaction();
		} else {
			gsmTransaction = mcidFlow.psi;
			if (gsmTransaction == null) {
				logger.warn("can't find SRI transaction by tid: " + tid);
				mcidFlow.psi = gsmTransaction = new GsmTransaction();
			}
		}

		String laiOrCid = proto.getTheField("CID_LAI_SAI");
		String result = "success";
		if (null == laiOrCid || laiOrCid.isEmpty())
			result = "fault";
		else {
			gsmTransaction.gsm_map_response.put("CID_LAI_SAI", laiOrCid);
			mcidFlow.x_CID_SAI_LAI = laiOrCid;
		}

		gsmTransaction.gsm_map_response.put("result", result);
		gsmTransaction.gsm_map_response.put("tid", tid);
		
		mcidFlow.x_psiResponseDelay = ((Date)proto.getField("x_timestamp")).getTime() - 				
				(null == mcidFlow.x_psiRequestTime ? 0L : mcidFlow.x_psiRequestTime);
		mcidFlow.x_result = result;		
		mcidFlow.x_psiResponse = true;
		si.saveObject(mcidFlow);
		tidMap.remove(tid);
	}

	void processPsiReq(ProtocolContainer proto, TcapInfo currentTcap) {

		// todo get McidFlow by "SRI.gsm_map_response.imsi"
		String imsi = proto.getTheField("imsi");
		if (null != imsi) {

			McidFlow mcidFlow = imsiMap.get(imsi);
			if (null == mcidFlow) {
				Map<String, Object> filter = new HashMap<String, Object>();
				filter.put("sri.gsm_map_response.imsi", imsi);
				mcidFlow = si.searchSingleObjects(McidFlow.class, filter);
			} else {
				logger.debug("Got sri transaction from cache for IMSI:" + imsi);
			}
			if (mcidFlow == null) {
				logger.warn("can't find PSI req by imsi: " + imsi);
				return;
			}

			McidFlow.GsmTransaction gsmTransaction = new McidFlow.GsmTransaction();
			String tid = currentTcap.tid;
			if (tid.isEmpty()) {
				logger.warn("SRI request have no tid");
				return;
			}

			gsmTransaction.gsm_map_request.put("tid", tid);
			mcidFlow.psi = gsmTransaction;
			
			mcidFlow.x_psiRequestTime = ((Date)proto.getField("x_timestamp")).getTime();
			
			si.saveObject(mcidFlow);
			tidMap.put(tid, mcidFlow);
						
			
		} else {
			logger.warn("There is NO imsi in PSI.request");
		}
	}

	void processSriResp(ProtocolContainer proto, TcapInfo currentTcap) {
		
		String tid = currentTcap.tid;
		String tidKey = "sri.gsm_map_request.tid";
		String imsi = proto.getTheField("imsi");

		McidFlow mcidFlow = getFlowByTid(tid, tidKey);
		McidFlow.GsmTransaction gsmTransaction;
		if (null != mcidFlow) {
			gsmTransaction = mcidFlow.sri;
			if (gsmTransaction == null) {
				logger.warn("can't find SRI transaction by tid: " + tid);
				mcidFlow.sri = gsmTransaction = new GsmTransaction();
			}
		} else {
			mcidFlow = new McidFlow();
			mcidFlow.sri = gsmTransaction = new GsmTransaction();
		}

		imsi = proto.getTheField("imsi");

		gsmTransaction.gsm_map_response.put("imsi", imsi);
		gsmTransaction.gsm_map_response.put("tid", tid);

		imsiMap.put(imsi, mcidFlow);
		tidMap.remove(tid);

		Object filestamp = proto.getField("x_timestamp");
		mcidFlow.x_sriResponseDelay = ((Date)filestamp).getTime() - 
				(null == mcidFlow.x_sriRequestTime ? 0L : mcidFlow.x_sriRequestTime);
		mcidFlow.x_IMSI = ""+proto.getField("IMSI");
		mcidFlow.x_sriResponse = true;		
		si.saveObject(mcidFlow);
	}

	private McidFlow getFlowByTid(String tid, String tidKey) {
		McidFlow mcidFlow = null;

		if (null == tid)
			logger.error("tid = NULL!");
		else {

			mcidFlow = tidMap.get(tid);
			if (null == mcidFlow) {
				Map<String, Object> filter = new HashMap<>();
				filter.put(tidKey, tid);
				List<McidFlow> mcidFlowL = si.searchObjects(McidFlow.class,
						filter);

				if (mcidFlowL.size() == 0) {
					logger.warn("can't find SRI req by tid: " + tid);
					return null;
				} else if (mcidFlowL.size() > 1) {
					logger.warn("There are "
							+ mcidFlowL.size()
							+ " MCid flows found by filter sri.gsm_map_request.tid="
							+ tid);
				}
				mcidFlow = mcidFlowL.get(0);
			} else {
				logger.debug("Got Mcidflow from cache by tid:" + tid);
			}
		}
		return mcidFlow;
	}

	void processSriReq(ProtocolContainer proto, TcapInfo currentTcap) {
		McidFlow mcidFlow = new McidFlow();
		McidFlow.GsmTransaction sriReq = new McidFlow.GsmTransaction();
		String tid = currentTcap.tid;
		if (tid.isEmpty()) {
			logger.warn("SRI request have no tid");
			return;
		}
		sriReq.gsm_map_request.put("tid", tid);
		mcidFlow.sri = sriReq;

		mcidFlow.x_sriRequestTime = ((Date)proto.getField("x_timestamp")).getTime();
		mcidFlow.x_timestamp = (Date)proto.getField("x_timestamp");		
		si.saveObject(mcidFlow);
		tidMap.put(tid, mcidFlow);
	}

	static DateTime parseDateTime(String text) {
		return new DateTime();//ISODateTimeFormat.dateTimeNoMillis().parseDateTime(text);		
	}

	static Logger logger = Logger
			.getLogger(McidPacketProcessor.class.getName());

}
