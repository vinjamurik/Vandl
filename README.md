### This is the repository for the Visual Analytics & Data Lake (VANDL) project.

**elastableau** - a nodejs app that allows Tableau Desktop 9.1 to interface directly with an Elasticsearch instance.  Intended to be used in the abscence of a Tableau Server backend.
	Requires NodeJS + NPM to be installed.  Within NPM, requires ExpressJS, Body-Parser (if using ExpressJS 4.x), and request.  Command to do so: "npm install express body-parser request".
	Start webapp using "node index.js" from within project directory.  Defaults to port 9001, but change it in the index.js file if you want to use another port.  From within Tableau Desktop 9.1,
	choose Web Data Connector from within Sources, point to localhost:(port), then choose the index, type, number of records, and click connect.  It will create an extract for you to work with locally.
	NOTE: this is not a data streaming solution, as that is accomplished via an open connection to ElasticSearch through Tableau Server.  This is the next best thing in the abscence of Tableau Server.

**agol_logs** - Scala code that interfaces with S3 Logs (in the x-globe-logs bucket) and transforms then ingests them to ElasticSearch or SparkSQL

**owa_logs** - Scala code that interfaces with MySQL scripts and ingests that data to ElasticSearch