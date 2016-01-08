sources:
-file: single text file that will be read in line by line to the sink
-directory: watches a directory for changes and appends them to the sink
-stream: polls a topic and places the result in the sink

sinks:
-file: writes data from the source to the file (overwrite)
-stream: publishes data from the source to the topic

path:
-the location of the file/directory in either the source/sink

topic:
-the name of the topic in the sink/source