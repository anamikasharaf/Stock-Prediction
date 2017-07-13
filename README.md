# Stock-Prediction

Predicting Yesterday's Stock Price:

Objective:

The objective of this lab is to create a streaming data pipeline using Apache Spark and Apache Kafka in which future stock prices are predicted based on historical data. Goal is to get the "plumbing" correct – not to accurately predict a stock price!

Data:

We will be using historical financial data from Yahoo! Finance. We can work with whichever stocks we want for the purpose of developing and testing this lab. In order to get all historical daily stock data for Apple from 2012 to present, for example, 

you type the following command in a terminal window on your sandbox:
 


$ wget http://ichart.yahoo.com/table.csv\?s=AAPL\&a=0\&b=1\&c=2012\&d=11\&e=31\&f=2017
 


The file generated has the following schema:

Date,Open,High,Low,Close,Volume,Adj Close
 



Note that the data is provided from most to least recent, so you will need to reverse sort the data in order to simulate our "real-time" pipeline.  Below command works on our linux VM:
 
$ sed -n '1!G;h;$p' <input-file> > <output-file>
 



Standalone Kafka Producer:



The JSON producer record must conform to the following sample:
 


{

"timestamp":"2012-01-30",

"open":28.190001,

"high":28.690001,

"low":28.02,

"close":28.6,

"volume":23294900

}

The syntax for running the standalone Java Kafka producer is given below:

java -cp CS185-jar-with-dependencies.jar Lab2.StockProducer localhost:9092 DATA/orcl.csv orcl prices 1000
 




Spark Streaming Application



NOTE: If you have not done so already, install the mapr-spark package on your sandbox.  As the root user, run the following 

commands:

vi /etc/yum.repos.d/mapr-eco.repo

--> change line containing baseurl to be equal to following:

baseurl=http://package.mapr.com/releases/MEP/MEP-2.0.0/redhat
 
Save the file and then run the following commands:

yum remove mapr-spark
yum clean all
yum install mapr-spark
 
 
Verify that the  version installed is 2.0.1 so it agrees with the Kafka API we specified in pom.xml:

rpm -qa | grep mapr-spark
 


The JSON producer record must conform to the following sample: 

{

"lastTimestamp":"2012-12-11",

"meanHigh":32.225999599999994,

"meanLow":31.783999799999997,

"meanOpen":32.0380006,

"meanClose":32.0719998,

"meanVolume":2.415158E7,

"lastClose":32.34

}

The syntax for running the Spark application is given below:

/opt/mapr/spark/spark-2.0.1/bin/spark-submit --class Lab2.StockSparkApp CS185-jar-with-dependencies.jar localhost:9092 local[2] prices stats mycg 5000
 




Standalone Kafka Consumer:



The value of the "aggregated statistic" metric is calculated as follows:
 
meanVolume * (meanHigh + meanLow + meanOpen + meanClose) / 4.0
 
Then when calculating the delta percentage (difference between the previous aggregated statistic and the current one), you need to divide by the meanVolume, as shown below:
 
(currentAggregatedStatistic – previousAggregatedStatistic) / ( 100 * meanVolume)
 
We must consider positive, negative, and zero values above to formulate the right plan to buy, sell, or hold.
Your consumer must output to the screen a line for each batch of records it gets from the Kafka topic using the following format:

lastTimestamp,stockSymbol,lastClose,deltaPercentage,position
 


Here's a sample of output using 0.01 percent as the threshold:

2014-05-09,orcl,41.040001,-0.11007555956311095,buy

2014-05-16,orcl,41.689999,0.10516324601700763,sell

2014-05-23,orcl,42.150002,-0.14378334854710764,buy

2014-06-02,orcl,41.970001,0.004958062178341045,hold

2014-06-09,orcl,42.700001,-0.047328194260115676,buy


 
Note when the delta percentage is positive and greater than the threshold, we recommend "sell".  When delta percentage is negative and absolute value is greater than the threshold, we recommend "buy".

The syntax for running the standalone Java Kafka consumer is given below:

java -cp CS185-jar-with-dependencies.jar Lab2.StockConsumer localhost:9092 stats orcl mygroup 0.01
