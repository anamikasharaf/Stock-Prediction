package Lab2;

import org.apache.kafka.clients.consumer.*;


import java.io.IOException;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StockConsumer {

    // Declare a new consumer
    public static KafkaConsumer<String, JsonNode> consumer;

    public static void main(String[] args) throws IOException {
        // check command-line arguments
        if(args.length != 5) {
            System.err.println("usage: StockConsumer <broker-socket> <input-topic> <stock-symbol> <group-id> <threshold-%>");
            System.err.println("e.g.: StockConsumer localhost:9092 stats orcl mycg 0.5");
            System.exit(1);
        }
        
        // initialize varaibles
        String brokerSocket = args[0];
        String inputTopic = args[1];
        String stockSymbol = args[2];
        String groupId = args[3];
        double thresholdPercentage = Double.parseDouble(args[4]);
        
        long pollTimeOut = 1000; //might need to adjust to 10000

        
        // configure consumer
        configureConsumer(brokerSocket, groupId);
        
        // subscribe to the topic
        List<String> topics = new ArrayList<>();
        topics.add(inputTopic);
        consumer.subscribe(topics);

        // loop infinitely -- pulling messages out every pollTimeOut ms
        JsonNode nextrecord;
        double currentAggregatedStatistic = 0, previousAggregatedStatistic = 0, deltapercentage = 0;
        String decision = null;
        int counter=0;
        
        while(true) {
        	
            // Request unread messages from the topic.
            ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(pollTimeOut);
            Iterator<ConsumerRecord<String, JsonNode>> iterator = consumerRecords.iterator();
            int count = 0;
            double sumHigh = 0, sumLow = 0, sumOpen = 0, sumClose = 0, lastClose=0;
            long sumVolume=0, meanVolume = 0;
            double meanHigh = 0, meanLow = 0, meanOpen = 0, meanClose = 0;
            String lastTimestamp = null;
            
            
            // iterate through message batch
            while (iterator.hasNext()) {
            		
            	    // create a ConsumerRecord from message
                    ConsumerRecord<String, JsonNode> record = iterator.next();
                 
                    // pull out statistics from message
                    nextrecord = record.value();
                    lastTimestamp = nextrecord.get("lastTimestamp").asText();
                    sumHigh += nextrecord.get("meanHigh").asDouble();
                    sumLow += nextrecord.get("meanLow").asDouble();
                    sumOpen += nextrecord.get("meanOpen").asDouble();
                    sumClose += nextrecord.get("meanClose").asDouble();
                    sumVolume += nextrecord.get("meanVolume").asLong();
                    lastClose = nextrecord.get("lastClose").asDouble();
                      	
                	count = count + 1;

             } 
            // calculate batch statistics meanHigh, meanLow, meanOpen, meanClose, meanVolume
            if(count!=0){
            	
	            meanHigh = sumHigh/count;
	            meanLow = sumLow/count;
	            meanOpen = sumOpen/count;
	            meanClose = sumClose/count;
	            meanVolume = sumVolume/count;
	            
	            if(counter == 0){
	            	
	            	//  calculate currentAggregatedStatistic and compare to previousAggregatedStatistic
		            currentAggregatedStatistic = (meanVolume * (meanHigh + meanLow + meanOpen + meanClose) )/ 4.0;
		            deltapercentage = currentAggregatedStatistic / (100 * meanVolume);
		            previousAggregatedStatistic = currentAggregatedStatistic;
		            counter++;
	            }
	            else{
	            	
	            	//  calculate currentAggregatedStatistic and compare to previousAggregatedStatistic
		            currentAggregatedStatistic = (meanVolume * (meanHigh + meanLow + meanOpen + meanClose)) / 4.0;
		            deltapercentage = (currentAggregatedStatistic - previousAggregatedStatistic)/(100*meanVolume);
		            previousAggregatedStatistic=currentAggregatedStatistic;
	            
	            }
	            
	            // determine if delta percentage is greater than threshold 
	            if(deltapercentage >= 0 && deltapercentage > thresholdPercentage){
	            	decision = "sell";
	            }
	            else if(deltapercentage < 0 && ((deltapercentage * -1) > thresholdPercentage)){
	            	decision = "buy";
	            }
	            else{
	            	decision = "hold";
	            }
	            
	            // print output to screen	            
	            System.out.println(lastTimestamp + "," + stockSymbol + "," + lastClose + "," + deltapercentage + "," + decision);  
	            
            }
            
            
        }
    }

    public static void configureConsumer(String brokerSocket, String groupId) {
        Properties props = new Properties();
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", brokerSocket);
        props.put("group.id", groupId);
        props.put("auto.commit.enable", true);

        consumer = new KafkaConsumer<String, JsonNode>(props);
    }
}

