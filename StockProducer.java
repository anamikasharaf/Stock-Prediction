package Lab2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
        
public class StockProducer {
    // Set the stream and topic to publish to.
    public static String topic;
    
        
    // Declare a new producer
    public static KafkaProducer<String, JsonNode> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // check command-line args
        if(args.length != 5) {
            System.err.println("usage: StockProducer <broker-socket> <input-file> <stock-symbol> <output-topic> <sleep-time>");
            System.err.println("eg: StockProducer localhost:9092 /user/user01/LAB2/orcl.csv orcl prices 1000");
            System.exit(1);
        }
        
        // initialize variables
        String brokerSocket = args[0];
        String inputFile = args[1];
        String stockSymbol = args[2];
        String outputTopic = args[3];
        long sleepTime = Long.parseLong(args[4]);
        
        
        // configure the producer
        configureProducer(brokerSocket);
        topic = args[3];
        
        // create a buffered file reader for the input file
        
        String line = "";
        String cvsSplitBy = ",";
        BufferedReader reader = null;
        
        try{
            reader = new BufferedReader(new FileReader(inputFile));
            
            // loop through all lines in input file
            while ((line = reader.readLine()) != null) {

                // using comma as separator
                String[] input_file = line.split(cvsSplitBy);
                
                //filter out "bad" records
                if(!input_file[0].equals("Date"))
                {

                	// create an ObjectNode to store data in
                	ObjectNode node = JsonNodeFactory.instance.objectNode();
                	
                	node.put("timestamp", input_file[0]);
                	node.put("open", input_file[1]);
                	node.put("high", input_file[2]);
                	node.put("low", input_file[3]);
                	node.put("close", input_file[4]);
                	node.put("volume", input_file[5]);
                	node.put("lastClose", input_file[6]);
	
                	ProducerRecord<String,JsonNode> record = new ProducerRecord<String, JsonNode>(topic,stockSymbol, node);
                
                	// parse out the fields from the line and create key-value pairs in ObjectNode
                	producer.send(record);
                
                	// sleep the thread
                	Thread.sleep(sleepTime);                
                }
                                           
            }
            
        }
        finally {
            if (reader != null) {
                try {
                	
                	// close buffered reader
                	reader.close();
                	
                	// close producer
                	producer.close();
                	
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, JsonNode>(props);
    }
}
