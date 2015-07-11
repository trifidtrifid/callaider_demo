package com.atalas.callaider.elastic.iface;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;


public class App 
{
	
	private static String[] filelds =  new String[]{ 
										"frame.time_epoch",        //timestamp MUST BE THE FIRST!!!
                                        "gsm_old.localValue",     
                                        "gsm_map.address.digits",  
                                        "tcap.otid", 
                                        "tcap.dtid", 
                                        "tcap.returnResultLast_element",
                                        "tcap.reject_element",
                                        "tcap.returnError_element",
                                        "tcap.begin_element",
                                        "tcap.abort_element",
                                        "tcap.continue_element",
                                        "tcap.dialogueAbort_element",
                                        "tcap.dialogueRequest_element",
                                        "tcap.end_element",
                                        "sccp.called.digits",
                                        "sccp.calling.digits"};
	private static Set<String> fileldsToIndex = 
			new HashSet<String>( Arrays.asList( new String[]{ 
										"frame.time_epoch",        //timestamp 
                                        "gsm_old.localValue",     
                                        "gsm_map.address.digits",  
                                        "tcap.otid", 
                                        "tcap.dtid", 
                                        "tcap.returnResultLast_element",
                                        "tcap.reject_element",
                                        "tcap.returnError_element",
                                        "tcap.begin_element",
                                        "tcap.abort_element",
                                        "tcap.continue_element",
                                        "tcap.dialogueAbort_element",
                                        "tcap.dialogueRequest_element",
                                        "tcap.end_element",
                                        "sccp.called.digits",
                                        "sccp.calling.digits"}));
	private static Set<String> mandatoryFilelds = 
			new HashSet<String>( Arrays.asList( new String[]{ 
					"gsm_old.localValue",     
                    "gsm_map.address.digits"}));
	
    public static void main( String[] args )
    {
    	/*BufferedReader br = 
                new BufferedReader(new InputStreamReader( new FileInputStream( args[1])));*/

    	//create array of objects
    	
    	/*Node builder = nodeBuilder().build();
		Node node = .node();
    	Client client = node.client();*/
    	Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    	
    	String argsLine = "";
    	for( String arg: args){argsLine+= " '" + arg +"'";}
    	System.out.println("========= Args.length="+args.length+" args: "+argsLine );
    	
    	try{
    		BufferedReader br = 
                          new BufferedReader(new InputStreamReader(
                        		  args.length == 0 || args[0].equals("-") ? System.in : new FileInputStream(args[0])));
    		
    		BulkRequestBuilder brb = new BulkRequestBuilder(client);
    		
    		List<Map<String,Object>> objects = createJSONObjects( br );
    		
    		for(Map<String,Object> obj : objects){
    			
    			String timestamp = ""+obj.get(filelds[0]);
    			//1398416027.259648000
    			
    			Calendar cldr = Calendar.getInstance();
				cldr.setTimeInMillis( Long.parseLong( timestamp.substring(0,10)) * 1000 + Integer.parseInt( timestamp.substring(11,14) ));
				obj.put("_timestamp", cldr.getTimeInMillis());
				brb.add( client.prepareIndex("network", "frame", null).setSource( obj ).setTimestamp(""+cldr.getTimeInMillis()));
				
    		}
    		BulkResponse result = brb.execute().actionGet();
    		result.buildFailureMessage();
    		System.out.println( "Result is:" + result.buildFailureMessage());
    		if( result.hasFailures()){
    			for( BulkItemResponse bir: result.getItems()){
    				if( bir.isFailed() ){
    					System.out.println( "\t" + bir.getFailureMessage());
    				}
    			}
    		}
    		
    	} catch(Exception io){
    		io.printStackTrace();
    		System.out.println("Error while red: "+io.getMessage());
    	} finally {
    		client.close();
    	}
    }
    private static Map<String,Object> timestampFormatMap = new HashMap<String,Object>();

	public static List<Map<String,Object>> createJSONObjects(BufferedReader br ) throws IOException {
		String input;   
		List<Map<String,Object>> objects = new ArrayList<Map<String,Object>>();
		
		while((input=br.readLine())!=null){
		
			boolean skipTheLine = false;
			String[] parts = input.split("[|]");
			
			Map<String,Object> lineObject = new HashMap<>();
			String mf = "";
			int mc = 0;
			for( int pidx = 0; pidx < parts.length; pidx ++){
				String part = parts[pidx].trim();
				String fldName = filelds[pidx];
				if( mandatoryFilelds.contains(fldName)){
					if("".equals(part) ){
						skipTheLine = true;						
						break; 
					} else {
						mf += fldName+"='"+part+"' ";
					}
					mc++;
				}
				if( !"".equals(part)){
					String name = pidx > 0 && fileldsToIndex.contains(fldName) ? "idx_"+fldName : fldName;
					try {
						lineObject.put(name, Long.parseLong(part)); 
					} catch (Exception x){
						lineObject.put(name,part);
					}					
				}
			}
			if(!skipTheLine && mc == mandatoryFilelds.size()){ //all of mandatory fields are not empty				
				objects.add(lineObject);
				
			} else {
				System.out.println("--- SKIPPED ["+mf+"]: "+input);
			}
		}
		return objects;
	}
	
}
