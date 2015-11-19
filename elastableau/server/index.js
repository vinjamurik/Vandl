var _ = require('underscore');
var express = require('express');
var request = require('request');
var bodyParser = require('body-parser');
var cookieParser = require('cookie-parser')
var multiparty = require('multiparty');
var fs = require('fs');
var Client = require('ssh2').Client;
var hdfs = require('webhdfs').createClient({user:'webuser',host:'172.31.60.102',port:50070,webhdfs:'/webhdfs/v1'});
var StringDecoder = require('string_decoder').StringDecoder;
var decoder = new StringDecoder('utf8');

var app = express();
var https = require('https');
var httpsOpt = {
  //pfx:fs.readFileSync('ssl/server.p12'),
  key:fs.readFileSync('ssl/server.key'),
  cert:fs.readFileSync('ssl/server.crt'),
  passphrase:'secret',
  requestCert:true,
  ca:[fs.readFileSync('ssl/ca_sign.crt'),fs.readFileSync('ssl/ca_root.crt')]
};
var port = process.env.PORT || 9001;
var url = 'http://172.31.82.218:9200/';//innovision
//var url = 'https://localhost:9200/';//local
var hdfsUrl = 'http://172.31.60.102:50070/webhdfs/v1';
var loginPath = '/proxy/login';

var getInternalUser = function(token){
  var map = {
    'amehta':'es_admin',
    'ncarolina':'east_coast_read'
  };
  return map[token];
};

var getElasticBatchSize = function(arg){
  return Math.min(50000,arg);
};

var proxyFunction = function(req,res,opt,func){
  _.extend(opt,httpsOpt);var token = req.socket.getPeerCertificate();
  token = token.subject ? token.subject.CN : req.query.cn;
  opt.auth = {user:getInternalUser(token),pass:'secret'};
  console.log('Attempting to proxy request to '+opt.url+'?'+JSON.stringify(opt.qs));
  request(opt,function(error,response,body){
    if(error) console.log(error);
    if(func) body = func(typeof body == 'string' ? JSON.parse(body) : body);
    res.set('cn',token);
    //res.set('Access-Control-Allow-Origin','*');
    //res.set('Access-Control-Allow-Headers','Content-Type');
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
    //host: '172.31.60.102',//innovision
    host: '172.31.62.40',//vserver
    port: 22,
    username: 'mehtaam',
    privateKey: fs.readFileSync('C:/Users/mehtaam/Documents/winscp575/GlobeServer_priv.ppk'),
    passphrase:'Aatlaron123456!'
  });
};

var getHdfsPath = function(req){
  var path = req.headers.filename || '';
  path = (req.headers.path || '/') + path;
  return '/xid'+path;
};

app.use(express.static('../site/'));
app.use(bodyParser.json());

/******************************************************************************************************************/

app.get('/proxy/elastic/preview',function(req,res){
  var options = {url:url+req.query.index+'/'+req.query.type+'/_search'};
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/data',function(req,res){
  if(+req.query.limit > 0){
    var options = {url:url+req.query.index+'/'+req.query.type+'/_search',qs:{from:+req.query.from,size:getElasticBatchSize(+req.query.limit-+req.query.from)}};
    if(req.query.random == 'true'){
      options.method = 'POST'; options.json = true; options.body = {"query":{"function_score":{"query":{"match_all":{}},"random_score":{}}}};
    }
  }else{
    var options = isNaN(req.query.from) ? {url:url+'_search/scroll',qs:{scroll:'1m',scroll_id:req.query.from}} : 
      {url:url+req.query.index+'/'+req.query.type+'/_search',qs:{scroll:'1m',size:'5000',search_type:'scan'}};
  }
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/indices',function(req,res){
  var options = {url: url+'_aliases'};
  proxyFunction(req,res,options);
});

app.get('/proxy/elastic/types',function(req,res){
  var options = {url:url+req.query.index+'/_mappings'};
  proxyFunction(req,res,options);
});

app.delete('/proxy/elastic/delete',function(req,res){
  var options = {url:url+req.query.index+'/'+(req.query.type || ''),method:'DELETE'};
  proxyFunction(req,res,options);
});

/******************************************************************************************************************/

app.get('/proxy/hdfs/dirStatus',function(req,res){
  var options = {url:hdfsUrl+getHdfsPath(req)+'?op=LISTSTATUS'};
  proxyFunction(req,res,options);
});

app.get('/proxy/hdfs/preview',function(req,res){
  var options = {url:hdfsUrl+getHdfsPath(req)+'?op=OPEN&length=1000'};
  proxyFunction(req,res,options);
});

app.delete('/proxy/hdfs/deleteFile',function(req,res){
  var options = {url:hdfsUrl+getHdfsPath(req)+'?op=DELETE&recursive=false',method:'DELETE'};
  proxyFunction(req,res,options);
});

app.post('/proxy/hdfs/upload',function(req,res){
  new multiparty.Form().parse(req,function(err,fields,files){
    var keys = Object.keys(files), finishCount = 0, file, wStream;
    for(var i=0;i<keys.length;i++){
      file = files[keys[i]][0];
      wStream = hdfs.createWriteStream(getHdfsPath(req)+file.originalFilename);      
      fs.createReadStream(file.path).pipe(wStream);
      wStream.on('finish',function(){
        finishCount++;
        if(finishCount == keys.length){
          res.end();
        }
      });
    }
  });
});

app.post('/proxy/hdfs/execute',function(req,res){
  var script = 'spark-submit elaspark.jar "'+req.headers.filetype+'" "'+getHdfsPath(req)+'" "'+req.headers.indexname.split('.')[0]+'" "'+(req.body.headers || '')+'"';
  executeScript(script,res);
});

/******************************************************************************************************************/

app.get('/proxy/s3/execute',function(req,res){
  var script = 'spark-submit elaspark.jar "'+req.headers.type+'"';
  executeScript(script,res);
});

/******************************************************************************************************************/

app.get('/proxy/logout',function(req,res){
  req.connection.renegotiate({requestCert:true});
  res.end();
});

var server = https.createServer(httpsOpt,app).listen(port,function(){
  console.log('Express server listening on port ' + server.address().port);
});

/******************************************************************************************************************/

module.exports = app;