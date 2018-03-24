# Stock-Prediction

Predicting Yesterday's Stock Price:

# Objective:

The objective of this project is to create a streaming data pipeline using Apache Spark and Apache Kafka in which future stock prices are predicted based on historical data. Goal is to get the "plumbing" correct – not to accurately predict a stock price!

# Data:

We will be using historical financial data from Yahoo! Finance. We can work with whichever stocks we want for the purpose of developing and testing this project. In order to get all historical daily stock data for Apple from 2012 to present, for example, you type the following command in a terminal window on your sandbox:


$ wget http://ichart.yahoo.com/table.csv\?s=APPL\&a=0\&b=1\&c=2012\&d=11\&e=31\&f=2017

The file generated has the following schema:

Date,Open,High,Low,Close,Volume,Adj Close
 
 
## Standalone Kafka Producer:

The JSON producer record's sample:
 
{

"timestamp":"2012-01-30",

"open":28.190001,

"high":28.690001,

"low":28.02,

"close":28.6,

"volume":23294900

}

## Spark Streaming Application

The JSON producer record's sample: 

{

"lastTimestamp":"2012-12-11",

"meanHigh":32.225999599999994,

"meanLow":31.783999799999997,

"meanOpen":32.0380006,

"meanClose":32.0719998,

"meanVolume":2.415158E7,

"lastClose":32.34

}

## Standalone Kafka Consumer:

The value of the "aggregated statistic" metric is calculated as follows:
 
meanVolume * (meanHigh + meanLow + meanOpen + meanClose) / 4.0
 
Then when calculating the delta percentage (difference between the previous aggregated statistic and the current one), divide by the meanVolume, as shown below:
 
(currentAggregatedStatistic – previousAggregatedStatistic) / ( 100 * meanVolume)
 
We must consider positive, negative, and zero values above to formulate the right plan to buy, sell, or hold.
Consumer output to the screen a line for each batch of records it gets from the Kafka topic using the following format:

lastTimestamp,stockSymbol,lastClose,deltaPercentage,position
 
## Output

Here's a sample of output using 0.01 percent as the threshold:


2014-05-09,orcl,41.040001,-0.11007555956311095,buy

2014-05-16,orcl,41.689999,0.10516324601700763,sell

2014-05-23,orcl,42.150002,-0.14378334854710764,buy

2014-06-02,orcl,41.970001,0.004958062178341045,hold

2014-06-09,orcl,42.700001,-0.047328194260115676,buy


 
Note when the delta percentage is positive and greater than the threshold, we recommend "sell".  When delta percentage is negative and absolute value is greater than the threshold, we recommend "buy".
