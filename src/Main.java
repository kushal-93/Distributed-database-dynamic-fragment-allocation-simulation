import java.sql.*;
import javax.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Main {

	public static void main(String[] args) {
		
		//String pointQueryFormat = "E:\\My workspace\\EclipseJavaWorkspace\\Fragment Allocation Using Threshold\\src\\point-query-format.txt";
		//String rangeQueryFormat = "E:\\My workspace\\EclipseJavaWorkspace\\Fragment Allocation Using Threshold\\src\\range-query-format.txt";
		String pointQuery = "E:\\My workspace\\EclipseJavaWorkspace\\Fragment Allocation Using Threshold\\src\\point-queries.txt";
		//String rangeQuery = "E:\\My workspace\\EclipseJavaWorkspace\\Fragment Allocation Using Threshold\\src\\range-queries.txt";
		
		/*ip addresses*/
		String sampatdIP = "10.100.53.25:3306";
		String rudradIP = "10.100.52.185:3306";
		String mydIP = "10.100.54.83:3306";
		String mylIP = "localhost:3306";
		
		/*sale_num to fragment map*/
		HashMap<Integer, Integer> saleNumStartToFrag = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> saleNumEndToFrag = new HashMap<Integer, Integer>();
		saleNumStartToFrag.put(1, 1);
		saleNumEndToFrag.put(1, 245);
		saleNumStartToFrag.put(2, 246);
		saleNumEndToFrag.put(2, 450);
		saleNumStartToFrag.put(3, 451);
		saleNumEndToFrag.put(3, 770);
		saleNumStartToFrag.put(4, 771);
		saleNumEndToFrag.put(4, 850);
		saleNumStartToFrag.put(5, 851);
		saleNumEndToFrag.put(5, 1000);
		
		/*random query generation*/
//		GenerateQueries genq = new GenerateQueries();
//		genq.makePointQueries(pointQueryFormat, pointQuery);
//		genq.makeRangeQueries(rangeQueryFormat, rangeQuery);
		
		int thresholdVal = 5;
		long unitTransmissionCostVal = 4; 
		int rowSizeVal = 344;
		
		/* Threshold algorithm execution */
		//ExecuteQuery exq = new ExecuteQuery();
		
		//exq.setupConnection(sampatdIP, rudradIP, mydIP, mylIP, thresholdVal, unitTransmissionCostVal, rowSizeVal);
		//exq.executePointQuery(pointQuery, saleNumStartToFrag, saleNumEndToFrag);
		
		/* TTCA algorithm execution */
		long T = 30;	//in seconds
		
		ExecuteQueryTTCA exqtt = new ExecuteQueryTTCA();
		exqtt.setupConnection(sampatdIP, rudradIP, mydIP, mylIP, thresholdVal, unitTransmissionCostVal, rowSizeVal, T);
		exqtt.executePointQuery(pointQuery, saleNumStartToFrag, saleNumEndToFrag);
		
	}

}
