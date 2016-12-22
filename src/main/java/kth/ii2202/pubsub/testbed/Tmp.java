package kth.ii2202.pubsub.testbed;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public class Tmp {

	public static void main(String[] args) throws Exception {
		Set<String> uniqueMsages = new HashSet<String>();
		PrintWriter writer = new PrintWriter("tmp/output.log", "UTF-8");
		
		try (BufferedReader br = new BufferedReader(new FileReader("tmp/testbed_long_sqs.log"))) {
		    String line;
		    String msgId;
		    while ((line = br.readLine()) != null) {
		    	msgId = getMsgId(line);
		    	if(uniqueMsages.contains(msgId)){
		    		
		    	} else {
		    		writer.println(line);
		    		uniqueMsages.add(msgId);
		    	}
		      
		    	
		    }
		}

		System.out.println("End");
		writer.close();
	}
	
	private static String getMsgId(String line) {
		String[] data = line.split(" ");
		return data[2];
	}

}
