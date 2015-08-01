package com.atalas.callaider.flow.mcid;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.atalas.callaider.elastic.iface.storage.StorageIndexedInterface;
import com.atalas.callaider.elastic.iface.storage.StorageInterface;

//класс будет паристь строки вида 
// 0 22/07/2015 23:51:20,
// 1 REQ_ID='20150722235118.6463s10.236.26.210p60333',
// 2 UID='controlcad',
// 3 PWD='AbrU43k',
// 4 MCC='',
// 5 MNC='',
// 6 CID='',
// 7 LAC='',
// 8 MODE='4',
// 9 MSISDN='79269004669',
// 10 IP=10.236.26.210,
// 11 RESULT=OK,
// 12 longitude=38.042580,
// 13 latitude=55.557743,
// 14 error=355,
// 15 age=0,
// 16 cid=50799,
// 17 lac=5012,
// 18 ,
// 29 comment="<LocationDatacomment=\"\"ageOfLocation=\"0\"locationInformation=\"-1\"/>"2
public class MLBSweb_app_logParser {
	
	private static Logger logger = Logger.getLogger(MLBSweb_app_logParser.class);
	private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	private final StorageInterface si;
	
	public static void main( String[] args )
    {
		MLBSweb_app_logParser app = new MLBSweb_app_logParser();
		if( args.length < 1 ){
			System.err.println("USage java -jar xxx.jar <input file name or - (stdin)>");
			return;
		}
		app.sdf.setTimeZone( TimeZone.getTimeZone("GMT+03:00"));

    	BasicConfigurator.configure();
    	
    	
    	String argsLine = "";
    	for( String arg: args){argsLine+= " '" + arg +"'";}
    	System.out.println("========= Args.length="+args.length+" args: "+argsLine );
    	
    	try {
			InputStream is = args[0].equals("-") ? System.in : new FileInputStream(args[0]);
			app.parseLogs( is );
		} catch (IOException e) {			
			e.printStackTrace();
		}
    }
	
	public MLBSweb_app_logParser() {
		this("network");
	}

	public MLBSweb_app_logParser(String indexName) {
		this.si = new StorageInterface(indexName);
	}


	public void parseLogs(InputStream is) throws IOException {
		BufferedReader br = 
                new BufferedReader(new InputStreamReader(is));
		String nextLine;
		while( null != (nextLine=br.readLine())){
			String[] parts = nextLine.split("[,]");
			if( parts.length < 12 ){
				logger.error("Failed to parse line: '"+nextLine+"' not enought parts ["+parts.length+" found but 12 required at least]");
				
			} else {
				Long msisdn = Long.parseLong(unquotePart(parts,9));
				String mode = unquotePart(parts,8);	
				String uid = unquotePart(parts,2);
				String result = unquotePart(parts,11);
				String lon, lat, err;
				if("OK".endsWith(result)){
					if( parts.length < 15 ){
						logger.error("Failed to parse(2) line: '"+nextLine+"' not enought parts ["+parts.length+" found but 15 required at least]");
						continue;
					}
					lon = unquotePart(parts,12);
					lat = unquotePart(parts,13);
					err = unquotePart(parts,14);
				} else {
					lon = lat = err = "0";
				}				
				
				Date reqDate;
				String unquotedDate = unquotePart(parts,0);
				try {
					reqDate = sdf.parse(unquotedDate);
					
					processTheEvent(msisdn, uid, reqDate, mode, lon, lat, err, result);
					
				} catch (ParseException e) {
					logger.error("INcorrect date format ["+unquotedDate+"]: "+e.getMessage(),e);
				}				
			}
		}
	}

	public void processTheEvent(Long msisdn, String uid, Date reqDate, String mode, String lon, String lat, String err, String result ) {
		try {
			McidFlow msf = getTheFlow(msisdn, reqDate);
			if( null!=msf ){
				msf.x_client = uid;
				msf.location = new StorageIndexedInterface.Location(Double.parseDouble(lon), Double.parseDouble(lat));
				msf.x_locError = Integer.parseInt(err);
				msf.x_serviceResult = result;
				msf.x_serviceMode = Integer.parseInt(mode);
				
				si.saveObject(msf);
			}
		} catch (Exception e) {
			logger.error("Failed to process event. "+e.getMessage(),e);
		}
	}
		
	private McidFlow getTheFlow(Long msisdn, Date rspDate) {
		if( null==rspDate || null==msisdn )
			return null;
		Map<String, Object> keyValueFilter = new HashMap<>();
		keyValueFilter.put("x_msisdn", msisdn);
		Calendar from = Calendar.getInstance();
		Calendar to = Calendar.getInstance();
		from.setTime(rspDate);
		from.add(Calendar.MINUTE, -1);
		to.setTime(rspDate);
		to.add(Calendar.SECOND, 1);
		
		keyValueFilter.put("x_timestamp", new Date[]{ from.getTime(), to.getTime()} );
		return si.searchSingleObjects(McidFlow.class,keyValueFilter,"x_timestamp", true);		
	}

	private String unquotePart(String[] parts, int pos) {
		String trimedPart = parts[pos].trim();
		int ioEq = trimedPart.indexOf("=");
		if( ioEq !=-1)
			trimedPart = trimedPart.substring(ioEq + 1);
			
		if(trimedPart.startsWith("'"))
			return trimedPart.substring(1,trimedPart.length()-1);
		return trimedPart;
	}
}