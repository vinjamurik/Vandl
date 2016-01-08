Elasticsearch:
-elastic(GET): lists all indices in cluster
-elastic/{index}(GET): lists all types in index
-elastic/{index}(DELETE): deletes an index
-elastic/{index}/{topic}(GET): gets the default number of items from the index/type
-elastic/{index}/{topic}/{from}/{finish}/{random}(GET): gets the documents from the specified bounds, in random order/non-random order
-elastic/{index}/{topic}/{scrollId}(GET): returns documents from index/type using the scan specified by the scrollId (tracked by elasticsearch).  
  If "-" is specified, it returns a new scrollId to be used in subsequent requests (each request returns a new scrollId to be used the next time)

Kafka:
-streaming/{topic}(POST): publishes the request body to the specified topic
-streaming/{topic}/{time}(GET): polls the topic for the specified number of milliseconds and returns the response