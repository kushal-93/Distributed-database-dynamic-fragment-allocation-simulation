import java.sql.*;
import javax.sql.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class GenerateQueries {
	
	static int min = 1;
	static int max = 1000;
	
	
	void makePointQueries(String format, String writeResult){
		try{
			BufferedReader br = new BufferedReader(new FileReader(format));
			BufferedWriter bw = new BufferedWriter(new FileWriter(writeResult));
			String line;
			while((line=br.readLine())!=null){
				for(int i=0;i<10;i++){
					int x = getRandomNumber(1, 1000);
					String xstr = Integer.toString(x);
					String query = line.replaceFirst("= x", "= "+xstr);
					System.out.println("Query: "+query);
					bw.write(query);
					bw.newLine();
				}
			}
			br.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	void makeRangeQueries(String format, String writeResult){
		try{
			BufferedReader br = new BufferedReader(new FileReader(format));
			BufferedWriter bw = new BufferedWriter(new FileWriter(writeResult));
			String line;
			while((line=br.readLine())!=null){
				for(int i=0;i<10;i++){
					int x = getRandomNumber(1, 999);
					String xstr = Integer.toString(x);
					int y = getRandomNumber(x, 1000);
					String ystr = Integer.toString(y);
					String temp = line.replaceFirst(" x ", " "+xstr+" ");
					String query = temp.replaceFirst("and y", "and "+ystr);
					System.out.println("Query: "+query);
					bw.write(query);
					bw.newLine();
				}
			}
			br.close();
			bw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	static int getRandomNumber(int min, int max){
		Random r = new Random();
		return r.nextInt((max-min)+1)+min;
	}
}
