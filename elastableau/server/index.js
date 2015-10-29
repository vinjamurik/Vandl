
var express = require('express');
var request = require('request');
var bodyParser = require('body-parser');
var cookieParser = require('cookie-parser')
var multiparty = require('multiparty');
var jwt = require('jsonwebtoken');
var fs = require('fs');
var Client = require('ssh2').Client;
var hdfs = require('webhdfs').createClient({user:'webuser',host:'172.31.60.102',port:50070,webhdfs:'/webhdfs/v1'});

var app = express();
var router = express.Router();
var port = process.env.PORT || 9001;
var url = 'http://172.31.82.218:9200/';
var hdfsUrl = 'http://172.31.60.102:50070/webhdfs/v1';
var loginPath = '/proxy/login';

var getElasticBatchSize = function(arg){
  return Math.min(50000,arg);
};

var proxyFunction = function(req,res,opt,func){
  console.log('Attempting to proxy request to '+opt.url+'?'+JSON.stringify(opt.qs));
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
app.use(router);

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

app.get('/proxy/hdfs/execute',function(req,res){
  var script = 'spark-submit elaspark.jar "'+req.headers.filetype+'" "'+getHdfsPath(req)+'" "'+req.headers.filename.split('.')[0]+'"';
  executeScript(script,res);
});

/******************************************************************************************************************/

app.get('/proxy/s3/execute',function(req,res){
  var script = 'spark-submit elaspark.jar "'+req.headers.type+'"';
  executeScript(script,res);
});

/******************************************************************************************************************/

app.post(loginPath,function(req,res){
  var token = jwt.sign(req.body.username,req.body.password,{expiresIn:'20m'});
  res.send(token);
});

/******************************************************************************************************************/

var server = app.listen(port,function(){
  console.log('Express server listening on port ' + server.address().port);
});

/******************************************************************************************************************/

router.use(function(req,res,next){
  try{
    if(req.url !== loginPath){
      jwt.verify((req.headers.authtoken || req.query.authToken || ''),'secret',{ignoreExpiration:false});
    }
    next();
  }catch(err){
    console.log(err, req.method);
    res.redirect(401,'/login');
  }
});

module.exports = app;