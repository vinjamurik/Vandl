var express = require('express'),request = require('request'),bodyParser = require('body-parser'),app = express(),port = process.env.PORT || 9001;
var elastic = {}
var url = 'http://172.31.82.218:9200/';
app.use(express.static('./'));
app.use(bodyParser.json());

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
      res.sendStatus(response.statusCode);
    }
  });
};

app.get('/', function (req, res) {
  res.sendFile('./index.html');
});

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

var server = app.listen(port, function () {
  console.log('Express server listening on port ' + server.address().port);
});

module.exports = app;