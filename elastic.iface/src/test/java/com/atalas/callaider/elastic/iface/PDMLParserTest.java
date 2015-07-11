/*package com.atalas.callaider.elastic.iface;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.atalas.callaider.elastic.iface.PDMLParser.FieldMapping;

public class PDMLParserTest {


	@Test
	public void testParse() {
		FieldMapping fieldsTree = createFieldsTree();
		try {
			//"c:\work\workspace\eclipse.jre1.8\vonline\elastic.iface\src\test\resources\ts.test.xml"
			InputStream fis = new FileInputStream("src/test/resources/ts.test.xml");
			PDMLParser pdp = new PDMLParser(fis, fieldsTree );
			List<Map<String, Object>> rslt = pdp.parse();
			assertEquals( rslt.size(), 2);
			assertNotNull( rslt.get(0).get("ip"));
			assertNotNull( rslt.get(0).get("frame"));
			assertNotNull( rslt.get(0).get("sctp"));
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	public static FieldMapping createFieldsTree() {
		
		FieldMapping ftFrame = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftFrame.lowerLevelMap.put("frame.time", new FieldMapping( FieldMapping.Type.STRING, null));
		ftFrame.lowerLevelMap.put("frame.protocols", new FieldMapping( FieldMapping.Type.STRING, null));
		
		FieldMapping ftIP = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftIP.lowerLevelMap.put("ip.version", new FieldMapping( FieldMapping.Type.STRING, null));
		ftIP.lowerLevelMap.put("ip.src", new FieldMapping( FieldMapping.Type.STRING, null));
		ftIP.lowerLevelMap.put("ip.dst", new FieldMapping( FieldMapping.Type.STRING, null));
		ftIP.lowerLevelMap.put("ip.proto", new FieldMapping( FieldMapping.Type.STRING, null));
		
		FieldMapping ftSCTP = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftSCTP.lowerLevelMap.put("sctp.srcport", new FieldMapping( FieldMapping.Type.STRING, null));
		ftSCTP.lowerLevelMap.put("sctp.dstport", new FieldMapping( FieldMapping.Type.STRING, null));
		ftSCTP.lowerLevelMap.put("sctp.port", new FieldMapping( FieldMapping.Type.STRING, null));
		ftSCTP.lowerLevelMap.put("sctp.assoc_index", new FieldMapping( FieldMapping.Type.STRING, null));
		
		FieldMapping ftRoot = new FieldMapping( FieldMapping.Type.CONTAINER, new HashMap<String, PDMLParser.FieldMapping>());
		ftRoot.lowerLevelMap.put( "frame", ftFrame );
		ftRoot.lowerLevelMap.put( "sctp", ftSCTP );
		ftRoot.lowerLevelMap.put( "ip", ftIP );
		return ftRoot;
	}

}
*/