import java.sql.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class ExecuteQuery {
	
	Connection connSkg = null;
    Connection connRpst = null;
    Connection connKm1 = null;
    Connection connKm2 = null;
    Connection[] connArr = new Connection[4];
    int thresholdValue;
    /*localhost value is the site num of that machine. Here, it is 1*/
    int localhost = 1;
    long unitTransmissionCost;
    int rowSize;
    long fragmentAllocCost[] = {0,0,0,0,0};
    int fragmentAllocNum[] = {0,0,0,0,0};
    long fragmentAllocTime[] = {0,0,0,0,0};
	
	void setupConnection(String sampatdIP, String rudradIP, String mydIP, String mylIP, int thresholdVal, long unitTransmissionCostVal, int rowSizeVal){
		try{
			rowSize = rowSizeVal;
			thresholdValue = thresholdVal;
			unitTransmissionCost = unitTransmissionCostVal;
			
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
				int numOfAccess = 0;
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
				System.out.println("=============== SAle number: "+saleNum);
				System.out.println("===============Fragment number: "+fragmentNum);
				
				if(fragmentNum != 0){
					
					/*getting site number for the fragment and the number of accesses made to the fragment*/
					Statement st = connArr[localhost-1].createStatement();
					String query = "SELECT SITE_NUM, NUM_OF_ACCESS FROM fragment_info WHERE FRAGMENT_NUM="+fragmentNum;
					ResultSet rs = st.executeQuery(query);
					while(rs.next()){									//changed
						siteNum = rs.getInt("SITE_NUM");
						numOfAccess = rs.getInt("NUM_OF_ACCESS");
					}
					
					System.out.println("============site number: "+siteNum);
					System.out.println("============access number: "+numOfAccess);
					
					//rs.close();
					
					/*connect to siteNum and retrieve result*/
					st = connArr[siteNum-1].createStatement();
					if(line.indexOf("select")>=0){
						/*point query execution and result*/							//changed
						String result = "";
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
						
					}
					st.close();
					
					/* update fragment_info table and migrate fragment if required */
					if(siteNum == localhost){
						System.out.println("================= local server access");
						/*fragment was in localhost hence no migration required*/
						String queryString = "UPDATE fragment_info SET NUM_OF_ACCESS=0 WHERE FRAGMENT_NUM="+fragmentNum;
						System.out.println("========="+queryString);
						for(int i=0;i<4;i++){
							Statement stmt = connArr[i].createStatement();
							stmt.executeUpdate(queryString);
							stmt.close();
						}
					}
					else{
						/* fragment was in remote host hence migration may be required */
						
						System.out.println("================= remote server access");
						
						if(numOfAccess == thresholdValue){
							/* this query breaches threshold. fragment migration to localhost required */
							
							/* get fragment from remote site and store in result-set */
							Statement stmt = connArr[siteNum-1].createStatement();
							System.out.println("==================Fragment number: "+fragmentNum);
							System.out.println("==================startsalenum: "+startSaleNum);
							System.out.println("==================endsalenum: "+endSaleNum);
							String queryString  = "SELECT * FROM sales WHERE SALE_NUM BETWEEN "+Integer.toString(startSaleNum)+" AND "+Integer.toString(endSaleNum);
							long startTime = System.nanoTime();
							//rst holds the fragment
							ResultSet rst = stmt.executeQuery(queryString);
							long endTime = System.nanoTime();
							
							long fragmentRetrieveTime = (endTime - startTime)/1000000; //in milliseconds
							long dataVolume = (long)(endSaleNum - startSaleNum + 1)*(long)rowSize; //in bytes
							long fragmentRetrieveCost = dataVolume * unitTransmissionCost;
							fragmentAllocCost[fragmentNum-1] += (2*fragmentRetrieveCost);
							fragmentAllocNum[fragmentNum-1] += 1;
							fragmentAllocTime[fragmentNum-1] += fragmentRetrieveTime;
							
							
							//update fragment_info table
							String updateFragQuery = "UPDATE fragment_info SET NUM_OF_ACCESS=0, SITE_NUM = "+localhost+" WHERE FRAGMENT_NUM="+fragmentNum;
							System.out.println("=============="+updateFragQuery);
							for(int j=0;j<4;j++){
								Statement upst = connArr[j].createStatement();
								upst.executeUpdate(updateFragQuery);
								upst.close();
							}			
						}
						else{
							/* threshold value is not breached hence no migration required */
							String upstQuery = "UPDATE fragment_info SET NUM_OF_ACCESS="+(numOfAccess+1)+" WHERE FRAGMENT_NUM="+fragmentNum;
							System.out.println("==========="+upstQuery);
							for(int i=0;i<4;i++){
								Statement upst = connArr[i].createStatement();
								upst.executeUpdate(upstQuery);
								upst.close();
							}
						}
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