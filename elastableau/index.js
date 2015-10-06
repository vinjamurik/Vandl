
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
var elastic = {}
var elasticTypeMap = {
  'long':'int',
  'double':'float'
};
var url = 'http://172.31.82.218:9200/';
var hdfsUrl = 'http://172.31.60.102:50070/webhdfs/v1';
/*var AWS = require('aws-sdk');
var s3 = new AWS.S3({
  accessKeyId:'AKIAJUFNNYKM4OFFQLBQ',
  secretAccessKey:'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'
});*/

var proxyFunction = function(req,res,opt,func){
  console.log('Attempting to proxy request to '+opt.url);
  request(opt,function(error,response,body){
    if(func) body = func(typeof body == 'string' ? JSON.parse(body) : body);
    res.set('Access-Control-Allow-Origin','*');
    res.set('Access-Control-Allow-Headers','Content-Type');
    res.send(body);
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
  var path = req.headers.filename || '';
  path = (req.headers.path || '/') + path;
  return '/xid'+path;
};

app.use(express.static('./'));
app.use(bodyParser.json());

app.get('/proxy/elastic/preview',function(req,res){
  var options = {url:url+req.query.index+'/'+req.query.type+'/_search'};
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/data',function(req,res){
  if(elastic.limit >= +req.query.from + elastic.size){
    var options = {
      url: url+elastic.index+'/'+elastic.type+'/_search?size='+elastic.size+'&from='+req.query.from
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
    res.set('Access-Control-Allow-Origin','*');
    res.send({hits:{hits:[]}});
  }
});

app.get('/proxy/elastic/indices',function(req,res){
  var options = {url: url+'_aliases'};
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/:index/types', function (req, res) {
  var options = {url: url+req.params.index+'/_mapping'};
  elastic.index = req.params.index;
  elastic.types = {};
  var func = function(body){
    var keys = Object.keys(body[elastic.index].mappings);
    for(var i=0 ; i < keys.length ; i++){
      var key = keys[i];
      elastic.types[key] = {headers:body[elastic.index].mappings[key].properties};
    }
    return elastic.types;
  };
  proxyFunction(req,res,options,func);
});

app.get('/proxy/elastic/headers',function(req,res){
  var func = function(body){
    body = {names:[],types:[]};
    var keys = Object.keys(elastic.types[elastic.type].headers);
    for(var i=0 ; i < keys.length ; i++){
      var key = keys[i];
      body.names.push(key);
      body.types.push(elastic.types[elastic.type].headers[key].type);
    }
    return body;
  };
  proxyFunction(req,res,{},func);
});

app.get('/proxy/elastic',function(req,res){
    require('util')._extend(elastic,req.query);
    elastic.size = Math.min(50000,elastic.limit);
    res.set('Access-Control-Allow-Origin','*');
    res.end();
});

app.get('/proxy/hdfs/dirStatus',function(req,res){
  var options = {url:hdfsUrl+getHdfsPath(req)+'?op=LISTSTATUS'};
  proxyFunction(req,res,options);
});

app.post('/proxy/hdfs/upload',function(req,res){
  new multiparty.Form().parse(req,function(err,fields,files){
    var keys = Object.keys(files);
    var finishCount = 0;
    for(var i=0;i<keys.length;i++){
      var file = files[keys[i]][0];
      var hdfsPath = getHdfsPath(req)+file.originalFilename;
      var rStream = fs.createReadStream(file.path);
      var wStream = hdfs.createWriteStream(hdfsPath);
      rStream.pipe(wStream);
      wStream.on('finish',function(){
        finishCount++;
        if(finishCount == keys.length){
          res.end();
        }
      });
    }
  });
});

app.get('/proxy/hdfs/execute',function(req,res){
  var script = 'spark-submit elaspark.jar "'+req.headers.filetype+'" "'+getHdfsPath(req)+'" "'+req.headers.filename.split('.')[0]+'"';
  executeScript(script,res);
});

var server = app.listen(port,function(){
  console.log('Express server listening on port ' + server.address().port);
});

module.exports = app;