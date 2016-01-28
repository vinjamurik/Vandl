angular.module('elastableau',['ui.bootstrap','ngFileUpload'])
.run(function($rootScope){
  
  $rootScope._CONFIG = _CONFIG;

  $rootScope.reset = function(factory,args){
    args = args || factory.resetArgs;
    angular.forEach(args,function(arg){
      switch(typeof factory[arg]){
        case 'object':
          factory[arg] = angular.isArray(factory[arg]) ? [] : {};
          break;
        case 'string':
          factory[arg] = '';
          break;
        case 'number':
          factory[arg] = 0;
          break;
        default:
          break;
      }
    });
  };

}).factory('ElasticFactory',function($http,$rootScope){

  tableau.connectionName = 'Elastableau';
	
  var factory = {
    url:_CONFIG.url+'analytics/',index:'',indices:[],type:'',types:[],data:[],headerNames:[],headerTypes:[],from:0,finish:0,count:0,random:false,connector:tableau.makeConnector(),
    resetArgs:['indices','types','data','headerNames','headerTypes','index','type'],connectionArgs:['index','type','from','finish','random','headerNames','headerTypes']
  };
  
  factory.getIndices = function(){
    $rootScope.reset(factory);
    return $http.get(factory.url).then(function(res){
      angular.forEach(angular.extend(res.data),function(v,k){
        factory.indices.push(k);
      });
      return res;
    });
  };
  factory.getIndices();

  factory.getTypes = function(){
    $rootScope.reset(factory,['type','types','count','headerNames','headerTypes','data']);
    return $http.get(factory.url+factory.index).then(function(res){
      angular.forEach(res.data[factory.index].mappings,function(v,k){
        factory.types.push(k)
      });
      return res;
    });
  };

  factory.generateColumnHeaders = function(){
    $rootScope.reset(factory,['headerNames','headerTypes']);
    if(factory.data.length){
      angular.forEach(factory.data[0],function(v,k){
        factory.headerNames.push(k);
        factory.headerTypes.push(typeof v);
      });
    }
  };

  factory.preview = function(){
    $rootScope.reset(factory,['count','data']);
    return $http.get(factory.url+factory.index+'/'+factory.type).then(function(res){
      factory.count = res.data.hits.total;
      angular.forEach(res.data.hits.hits,function(hit){
        factory.data.push(hit._source);
      });
      factory.generateColumnHeaders();
      return res;
    });
  };

  factory.extract = function(){
    var c = {};
    angular.forEach(factory.connectionArgs,function(arg){
      c[arg] = factory[arg];
    });
    c._scroll_id = (c.from === 0 && c.finish === 0) ? '-' : undefined;
    tableau.connectionData = JSON.stringify(c);
    $rootScope.reset(factory);
    tableau.submit();
  };

  factory.delete = function(){
    $http.delete(factory.url+factory.index).then(function(res){
      factory.getIndices();
    });
  };

  factory.connector.getColumnHeaders = function(){
    var c = JSON.parse(tableau.connectionData);
    tableau.headersCallback(c.headerNames,c.headerTypes);
  };

  factory.connector.getTableData = function(from){
    var c = JSON.parse(tableau.connectionData), data = [], url = factory.url+c.index+'/'+c.type+'/'+(c._scroll_id || c.from+'/'+c.finish+'/'+c.random);
    $http.get(url).then(function(res){
      angular.forEach(res.data.hits.hits,function(hit){
        data.push(hit._source);
      });
      c.from += data.length;
      c._scroll_id = res.data._scroll_id || c._scroll_id;
      c.finish = c.finish || res.data.hits.total;
      tableau.connectionData = JSON.stringify(c);
      tableau.dataCallback(data, c.from, c.from < c.finish && (c._scroll_id || !c.random));
    });
  };

  tableau.registerConnector(factory.connector);

  return factory;

}).controller('ElasticController',function($scope,ElasticFactory){

	$scope.ef = ElasticFactory;

}).factory('lake',function(Upload,$http,$uibModal){
  var factory = {};
  factory.url = _CONFIG.url+'lake/';
  factory.bucket = '';
  factory.buckets = [];
  factory.file = {};
  factory.objects = [];
  factory.prefix = '';
  
  factory.upload = function(file){
    Upload.upload({url:factory.url+'create/'+factory.bucket+'/'+file.name,file:file}).then(function(res){
      console.log(res);
    });
  };

  factory.getBuckets = function(){
    $http.get(factory.url+'buckets').then(function(res){
      factory.buckets = res.data;
    });
  };
  factory.getBuckets();

  factory.getObjects = function(){
    $http.get(factory.url+factory.bucket+'/list',{params:{prefix:factory.prefix}}).then(function(res){
      factory.objects = res.data;
    });
  };

  return factory;

}).factory('streaming',function($http,$interval,$rootScope){
  var factory = {};
  factory.url = _CONFIG.url+'stream/';
  factory.topic = '';
  factory.message = '';
  factory.topics = [];
  factory.time = 10;
  factory.messages = [];

  factory.getTopics = function(){
    $http.get(factory.url+'list').then(function(res){
      factory.topics = res.data;
    });
  };
  factory.getTopics();

  factory.poll = function(){
    return $http.get(factory.url+factory.topic+'/'+factory.time).then(function(res){
      factory.messages = res.data;
      console.log(factory.messages[0]);
    });
    return res;
  };

  factory.send = function(){
    return $http.post(factory.url+factory.topic,factory.message).then(function(res){
      factory.message = '';
      if(factory.topics.indexOf(factory.topic) == -1){
        factory.topics.push(factory.topic);
      }
      return res;
    });
  };

  return factory;

}).controller('ingest',function($scope,lake,streaming){
  $scope.lake = lake;
  $scope.streaming = streaming;

}).factory('visualize',function(){
  var factory = {};
  factory.images = [
    {
      img:'assets/img/kibana.png',
      href:'http://172.31.84.221:5601'
    },
    {
      img:'assets/img/plotly.png',
      href:''
    },
    {
      img:'assets/img/tableau.png',
      href:'http://172.31.9.66'
    },
    {
      img:'assets/img/bokeh.png',
      href:'http://172.31.48.155'
    },
    {
      img:'assets/img/rshiny.png',
      href:''
    },
    {
      img:'assets/img/paxata.png',
      href:'https://pri-poc.paxata.com'
    }
  ];

  return factory;

}).controller('visualize',function(visualize,$scope){
  $scope.visualize = visualize;
});