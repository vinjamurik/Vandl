
var express = require('express');
var request = require('request');
var bodyParser = require('body-parser');
var multiparty = require('multiparty');
var app = express();
var port = process.env.PORT || 9001;
var fs = require('fs');
var Client = require('ssh2').Client;
var hdfs = require('webhdfs').createClient({
  user:'webuser',
  host:'172.31.60.102',
  port:50070,
  webhdfs:'/webhdfs/v1'
});
var form = new multiparty.Form();
var elastic = {}
var url = 'http://172.31.82.218:9200/';
var hdfsUrl = 'http://172.31.60.102:50070/webhdfs/v1';

var proxyFunction = function(req,res,options){
  console.log('Attempting to proxy request to ' + options.url);
  request(options, function (error, response, body) {
    var header;
    if (!error && response.statusCode == 200){
      for (header in response.headers) {
        if (response.headers.hasOwnProperty(header)) {
          res.set(header, response.headers[header]);
        }
      }
      res.send(body);
    }
    else {
      error = error || response.statusMessage || response.statusCode;
      console.log('Error fulfilling request: "' + error.toString() + '"');
      res.end();
    }
  });
};

var executeScript = function(script,res){
  var conn = new Client();
  conn.on('ready', function() {
    console.log('Client :: ready');
    conn.exec(script, function(err, stream) {
      if (err) throw err;
      stream.on('close', function(code, signal) {
        console.log('Stream :: close :: code: ' + code + ', signal: ' + signal);
        conn.end();
        res.end();
      }).on('data', function(data) {
        console.log('STDOUT: ' + data);
      }).stderr.on('data', function(data) {
        console.log('STDERR: ' + data);
      });
    });
  }).connect({
    host: '172.31.60.102',
    port: 22,
    username: 'mehtaam',
    privateKey: require('fs').readFileSync('C:/Users/mehtaam/Documents/winscp575/GlobeServer_priv.ppk'),
    passphrase:'Aatlaron123456!'
  });
};

var getHdfsPath = function(req){
  return '/xid'+(req.headers.path || '/');
};

app.use(express.static('./'));
app.use(bodyParser.json());

app.get('/proxy/elastic/preview',function(req,res){
  var options = {
    url: url+req.query.index+'/'+req.query.type+'/_search',
    headers: {
      Accept: 'application/json',
      'User-Agent': 'elastableau/0.0.0'
    }
  };
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/data', function (req, res) {
  if(elastic.limit >= +req.query.from + elastic.size){
    var options = {
      url: url+elastic.index+'/'+elastic.type+'/_search?size='+elastic.size+'&from='+req.query.from,
      headers: {
        Accept: 'application/json',
        'User-Agent': 'elastableau/0.0.0'
      }
    };
    if(elastic.random){
      options.method = 'POST';
      options.json = true;
      options.body = {
        "query": {
          "function_score" : {
            "query" : { "match_all": {} },
            "random_score" : {}
          }
        }
      };
    }
    proxyFunction(req,res,options);
  }else{
    res.send({hits:{hits:[]}});
  }
});

app.get('/proxy/elastic/indices', function (req, res) {
  var options = {
    url: url+'_aliases',
    headers: {
      Accept: 'application/json',
      'User-Agent': 'elastableau/0.0.0'
    }
  };
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/:index/types', function (req, res) {
  var options = {
    url: url+req.params.index+'/_mapping',
    headers: {
      Accept: 'application/json',
      'User-Agent': 'elastableau/0.0.0'
    }
  };
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/headers',function(req,res){
  res.send(elastic.headers);
});

app.post('/proxy/elastic',function(req,res){
    elastic = req.body;
    elastic.size = Math.min(50000,elastic.limit);
    res.end();
});

app.get('/proxy/hdfs/dirStatus',function(req,res){
  var options = {url:hdfsUrl+getHdfsPath(req)+'?op=LISTSTATUS'};
  proxyFunction(req,res,options);
});

app.post('/proxy/hdfs/upload',function(req,res){
  form.parse(req,function(err,fields,files){
    var keys = Object.keys(files);
    for(var i=0;i<keys.length;i++){
      var file = files[keys[i]][0];
      var hdfsPath = getHdfsPath(req)+file.originalFilename;
      fs.createReadStream(file.path).pipe(hdfs.createWriteStream(hdfsPath));
    }
  });
  res.end();
});

app.get('/proxy/hdfs/execute',function(req,res){
  var script = 'spark-submit elaspark.jar "MySql" "'+getHdfsPath(req)+'"';
  executeScript(script,res);
});

var server = app.listen(port,function () {
  console.log('Express server listening on port ' + server.address().port);
});

module.exports = app;