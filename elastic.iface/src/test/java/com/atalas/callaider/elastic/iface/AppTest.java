package com.atalas.callaider.elastic.iface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	try {
			BufferedReader br = new BufferedReader(new StringReader(
					"Apr 25, 2014 10:53:47.040659000 CEST|22|79206500254,79201900681||80:d2:28:11|1||||||||1|79219900096|79219600019\n"+
					"Apr 25, 2014 10:53:47.041160000 CEST|45|79266167267,79219900041|93:d2:28:11|||||1||||1||79266167267|79219900096\n"+
					"Apr 25, 2014 10:53:47.041284000 CEST|22|79260865977,79219900041|94:d2:28:11|||||1||||1||79260865977|79219900096\n")); 
			try {
				List<Map<String,Object>> createdObject = App.createJSONObjects(br);
			} catch (IOException e) {				
				e.printStackTrace();
			}
		} catch (Exception e) {			
			e.printStackTrace();
			assertTrue( false );
		}
        assertTrue( true );
    }
}
