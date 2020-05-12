import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class ExecuteQueryTTCA {

    Connection[] connArr = new Connection[4];
    int thresholdValue;
    long T; 
    
    /*localhost value is the site num of that machine. Here, it is 1*/
    int localhost = 1;
    long unitTransmissionCost;
    int rowSize;
    long fragmentAllocCost[] = {0,0,0,0,0};
    int fragmentAllocNum[] = {0,0,0,0,0};
    
    long fragmentAllocTime[] = {0,0,0,0,0};
    int fragmentAccessCounter[] = {0,0,0,0,0};
    
    List<Long>[] fragAccessTimeTrack = new List[5];
    
	void setupConnection(String sampatdIP, String rudradIP, String mydIP, String mylIP, int thresholdVal, long unitTransmissionCostVal, int rowSizeVal, long TVal){
		try{
			Connection connSkg = null;
		    Connection connRpst = null;
		    Connection connKm1 = null;
		    Connection connKm2 = null;
			
			
			rowSize = rowSizeVal;
			thresholdValue = thresholdVal;
			unitTransmissionCost = unitTransmissionCostVal;
			T = TVal;
			for(int li=0;li<5; li++){
				fragAccessTimeTrack[li] = new ArrayList<>();
			}
			
			//my laptop - localhost -- site 1
	        String urlKm1 = "jdbc:mysql://"+mylIP+"/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	        Class.forName ("com.mysql.cj.jdbc.Driver");
	        //long startTime = System.nanoTime();
	        connKm1 = DriverManager.getConnection (urlKm1,"","");
         	System.out.println ("Database connection to my laptop established");
         	connArr[0] = connKm1;
			
			//sampat's desktop -- site 2 
			String urlSkg = "jdbc:mysql://"+sampatdIP+"/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
			Class.forName ("com.mysql.cj.jdbc.Driver");
	        //long startTime = System.nanoTime();
	        connSkg = DriverManager.getConnection (urlSkg,"","");
	        System.out.println ("Database connection to SKG established");
	        connArr[1] = connSkg;
	        
	        //my desktop -- site 3 
	        String urlKm2 = "jdbc:mysql://"+mydIP+"/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	        Class.forName ("com.mysql.cj.jdbc.Driver");
	        //long startTime = System.nanoTime();
	        connKm2 = DriverManager.getConnection (urlKm2,"","");
	        System.out.println ("Database connection to my desktop established");
	        connArr[2] = connKm2;
	        
	        //rudra's desktop -- site 4
	        String urlRpst = "jdbc:mysql://"+rudradIP+"/test?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	        Class.forName ("com.mysql.cj.jdbc.Driver");
	        //long startTime = System.nanoTime();
	        connRpst = DriverManager.getConnection (urlRpst,"","");
	        System.out.println ("Database connection to RPST established");
	        connArr[3] = connRpst;	        
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	//execution
	void executePointQuery(String queryFilePath, HashMap saleNumStartToFrag, HashMap saleNumEndToFrag){
		try{
			BufferedReader br = new BufferedReader(new FileReader(queryFilePath));
			String line;
			String ln;
			
			List<String> queryLines = new ArrayList<>();
			while((ln=br.readLine())!=null && ln.length()>1){
				queryLines.add(ln);
			}
			
			Scanner sc = new Scanner(System.in);
			int input = 0;
			
			for(int count=0; count<queryLines.size(); count++){
				
				while((count+1)%10==0){
					System.out.println("Enter 1 to continue: ");
					input = sc.nextInt();
					if(input == 1)
						break;
				}
				input = 0;
				
				line = queryLines.get(count);
				int len = line.length();
				int index = line.indexOf("=");
				String salestr = line.substring(index+1);
				int saleNum = Integer.parseInt(salestr);
				int fragmentNum = 0;
				int siteNum = 0;
				//int numOfAccess = 0;
				int startSaleNum = 0, endSaleNum = 0;
				
				/*Get fragment number for the query*/
				for(int i=1;i<=5;i++){
					startSaleNum = (int)saleNumStartToFrag.get(i);
					endSaleNum = (int)saleNumEndToFrag.get(i);
					if(saleNum>=startSaleNum && saleNum<=endSaleNum){
						fragmentNum = i;
						break;
					}
				}
				System.out.println("=====================\n\n=============== Sale number: "+saleNum);
				
				if(fragmentNum != 0){
					
					/* getting site number for the fragment */
					Statement st = connArr[localhost-1].createStatement();
					String query = "SELECT SITE_NUM FROM fragment_info WHERE FRAGMENT_NUM="+fragmentNum;
					ResultSet rs = st.executeQuery(query);
					while(rs.next()){									
						siteNum = rs.getInt("SITE_NUM");
					}
					System.out.println("============site number: "+siteNum);
					System.out.println("============fragment number: "+fragmentNum);
					
					/* connect to siteNum, retrieve result and update access counter */
					st = connArr[siteNum-1].createStatement();
					if(line.indexOf("select")>=0){
						/*point query execution and result*/							//changed
						String result = "";
						long accessTime = System.currentTimeMillis()/1000;              // in seconds
						ResultSet rst = st.executeQuery(line);
						System.out.println("\n------\n"+count+" :-> ");
						while(rst.next()){
							ResultSetMetaData rsmd = rst.getMetaData();
							int colNums = rsmd.getColumnCount();
							for(int colIter=1; colIter<=colNums; colIter++){ 			//changed
								int type = rsmd.getColumnType(colIter);
								if(type == Types.VARCHAR){
									System.out.print(" "+rst.getString(colIter));
								}
								else if(type == Types.INTEGER){
									System.out.print(" "+rst.getInt(colIter));
								}
								else if(type == Types.DOUBLE){
									System.out.print(" "+rst.getDouble(colIter));
								}
								else{
									System.out.print("unsupported datatype");
								}
								System.out.println();
							}
						}
						rst.close();
						
						/* store access counter and access time corresponding to the fragment */
						fragmentAccessCounter[fragmentNum-1]+=1;
						fragAccessTimeTrack[fragmentNum-1].add(accessTime);
						
						System.out.println("=============access counter: "+Arrays.toString(fragmentAccessCounter));
						System.out.println("=============time tracker: ");
						System.out.println(Arrays.deepToString(fragAccessTimeTrack));
					}
					st.close();
					
					/* check whether remote or local access was made */
					System.out.println("============ Localhost: "+localhost);
					if(siteNum != localhost){
						/* remote access */
						System.out.println("================= remote server access");
						
						if(fragmentAccessCounter[fragmentNum-1] > thresholdValue){
							/* threshold breached */
							int listSize = fragAccessTimeTrack[fragmentNum-1].size();
							long lasttthAccessTime = fragAccessTimeTrack[fragmentNum-1].get(listSize - (thresholdValue+1));
							long lastAccessTime = fragAccessTimeTrack[fragmentNum-1].get(listSize-1);
							
							System.out.println("============ (t+1)th access time: "+lasttthAccessTime);
							System.out.println("============ last access time: "+lastAccessTime);
							
							if((lastAccessTime - lasttthAccessTime) <= T){
								/* last t+1 accesses made within T time */
								//reset the counter to 0
								fragmentAccessCounter[fragmentNum-1] = 0;
								
								/* get fragment from remote site and store in result-set */
								Statement stmt = connArr[siteNum-1].createStatement();
								System.out.println("==================Fragment number: "+fragmentNum);
								System.out.println("==================startsalenum: "+startSaleNum);
								System.out.println("==================endsalenum: "+endSaleNum);
								String queryString  = "SELECT * FROM sales WHERE SALE_NUM BETWEEN "+Integer.toString(startSaleNum)+" AND "+Integer.toString(endSaleNum);
								long startTime = System.nanoTime();
								// rst holds the fragment
								ResultSet rst = stmt.executeQuery(queryString);
								long endTime = System.nanoTime();
								
								long fragmentRetrieveTime = (endTime - startTime)/1000000; //in milliseconds
								long dataVolume = (long)(endSaleNum - startSaleNum + 1)*(long)rowSize; //in bytes
								long fragmentRetrieveCost = dataVolume * unitTransmissionCost;
								fragmentAllocCost[fragmentNum-1] += (2*fragmentRetrieveCost);
								fragmentAllocNum[fragmentNum-1] += 1;
								fragmentAllocTime[fragmentNum-1] += fragmentRetrieveTime;
								
								// update fragment_info table
								String updateFragQuery = "UPDATE fragment_info SET SITE_NUM = "+localhost+" WHERE FRAGMENT_NUM="+fragmentNum;
								System.out.println("============== "+updateFragQuery);
								for(int j=0;j<4;j++){
									Statement upst = connArr[j].createStatement();
									upst.executeUpdate(updateFragQuery);
									upst.close();
								}
								stmt.close();
								rst.close();
							}
						}
					}
					else{
						System.out.println("================= Local server access");
						//nothing else to do 
					}
					
					
					
				}
				
				else{
					System.out.println("No fragment found for query.");
				}
				
			}
			//show result: 
			System.out.println("\n----------------------------\nResults: ");
			System.out.println("--------------------------------------------------------------");
			System.out.println("fragments allocation cost: "+Arrays.toString(fragmentAllocCost));
			System.out.println("fragments allocation number: "+Arrays.toString(fragmentAllocNum));
			System.out.println("fragments allocation time: "+Arrays.toString(fragmentAllocTime));
			System.out.println("--------------------------------------------------------------");
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
