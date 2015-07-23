package com.atalas.callaider.flow.mcid;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.atalas.callaider.elastic.iface.storage.StorageInterface;
import com.atalas.callaider.flow.mcid.McidFlow.Location;

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
// 12 RESULT=OK,
// 13 longitude=38.042580,
// 14 latitude=55.557743,
// 15 error=355,
// 16 age=0,
// 17 cid=50799,
// 18 lac=5012,
// 19 ,
// 20 comment="<LocationDatacomment=\"\"ageOfLocation=\"0\"locationInformation=\"-1\"/>"2
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
			if( 16 > parts.length ){
				logger.error("Failed to parse line: '"+nextLine+"' not enought parts");
				
			} else {
				String msisdn = unquotePart(parts,9);				
				String uid = unquotePart(parts,2);
				String lon = unquotePart(parts,13);
				String lat = unquotePart(parts,14);
				String err = unquotePart(parts,15);
				
				Date reqDate;
				String unquotedDate = unquotePart(parts,0);
				try {
					reqDate = sdf.parse(unquotedDate);
				} catch (ParseException e) {
					throw new IOException("INcorrect date format ["+unquotedDate+"]: "+e.getMessage(),e);
				}
				
				processTheEvent(msisdn, uid, reqDate, lon, lat, err);
			}
		}
	}

	public void processTheEvent(String msisdn, String uid, Date reqDate, String lon, String lat, String err) {
		McidFlow msf = getTheFlow(msisdn, reqDate);
		if( null!=msf ){
			msf.x_client = uid;
			msf.location = new Location(Double.parseDouble(lon), 
					Double.parseDouble(lat), Integer.parseInt(err));
			si.saveObject(msf);
		}
	}
	
	private McidFlow getTheFlow(String msisdn, Date rspDate) {
		Map<String, Object> keyValueFilter = new HashMap<>();
		keyValueFilter.put("x_msisdn", msisdn);
		keyValueFilter.put("x_timestamp", new Date[]{ new Date( rspDate.getTime() - 180000L), rspDate } );
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