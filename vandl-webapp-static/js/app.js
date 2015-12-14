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
    url:_CONFIG.url+'elastic/',index:'',indices:[],type:'',types:[],data:[],headerNames:[],headerTypes:[],from:0,finish:0,count:0,random:false,connector:tableau.makeConnector(),
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

}).factory('hdfs',function(Upload,$http,$uibModal){
  var factory = {};
  factory.url = _CONFIG.url+'/hdfs/';
  factory.dirFiles = [];
  factory.uploadFiles = [];
  factory.path = '/';
  factory.currentFolder = '';
  factory.fileType = '';
  factory.uploadDisabled = true;
  factory.preview = {text:'',headers:'',item:{},title:''};

  factory.getHumanizedSize = function(size,dPlaces){
    var g = 0;
    while(size/Math.pow(1000,g) > 1000){
      g++;
    }
    dPlaces = Math.pow(10,dPlaces);
    size /= Math.pow(1000,g);
    return Math.round(size*dPlaces)/dPlaces+' '+_CONFIG.fileSize.groups[g];
  };

  factory.endsWith = function(path,char){
    return path[path.length-1] == (char || '/');
  };

  factory.changeType = function(){
    factory.fileType = !factory.currentFolder || !factory.fileType ? factory.currentFolder : factory.fileType;
  };

  factory.changeFolder = function(){
    var match = factory.path.match(/.*\/(.*)\/$/) || factory.path.match(/.*\/(.*)$/) || ['',''];
    factory.currentFolder = match[1];
    factory.changeType();
  };

  factory.changeUploadDisabled = function(){
    factory.uploadDisabled = !factory.currentFolder || !factory.uploadFiles || !factory.uploadFiles.length;
  };

  factory.goIntoFolder = function(folder){
    factory.path += (factory.endsWith(factory.path) ? '' : '/')+folder.pathSuffix+'/';
    factory.on.pathChange();
  };

  factory.getOutOfFolder = function(){
    factory.path = factory.path.substring(0,factory.path.lastIndexOf(factory.currentFolder+(factory.endsWith(factory.path) ? '/' : '')));
    factory.on.pathChange();
  };

  factory.on = {
    fileChange:function(){
      angular.forEach(factory.uploadFiles,function(f){
        f.humanSize = factory.getHumanizedSize(f.size,3);
      });
      factory.changeUploadDisabled();
    },
    removeItem:function($index){
      factory.uploadFiles.splice($index,1);
      factory.changeUploadDisabled();
    },
    pathChange:function(){
      factory.path = factory.path || '/';
      factory.changeFolder();
      $http.get(factory.url+'dirStatus',{headers:{'path':factory.path}}).then(function(response){
        factory.showFolderOut = factory.path.indexOf('/') + 1;
        angular.copy([],factory.dirFiles);
        if(response.data.FileStatuses){
          angular.forEach(response.data.FileStatuses.FileStatus,function(item){
            item.humanSize = !!item.length ? factory.getHumanizedSize(item.length,3) : '0 '+_CONFIG.fileSize.groups[0];
            factory.dirFiles.push(item);
          });
        }
        factory.changeUploadDisabled();
      });
    },
    deleteFile:function(item){
      factory.path = factory.path || '/';
      $http.delete(factory.url+'deleteFile',{headers:{'path':factory.path,'filename':item.pathSuffix}}).then(function(res){
        factory.on.pathChange();
      });
    }
  };
  //factory.on.pathChange();

  factory.upload = function(){
    factory.path+=(factory.endsWith(factory.path) ? '' : '/');
    Upload.upload({
      url:factory.url+'upload',
      file:factory.uploadFiles,
      headers:{'path':factory.path,'filetype':factory.fileType}
    }).progress(function(evt){

    }).success(function(){
      angular.copy([],factory.uploadFiles);
      factory.on.pathChange();
    });
  };

  factory.ingest = function(item,manual){
    item = item || factory.preview.item;
    if(manual){
      factory.preview(item).then(function(res){
        $uibModal.open({
          animation: true,
          templateUrl: 'ingestModal.html',
          controller: 'ingest',
          size:'lg'
        });
        factory.preview.text = res.data;
        factory.preview.item = item;
        factory.preview.title = item.pathSuffix.match(/(.+)\./)[1];
      });
    }else{
      item.$executing = true;
      $http.post(factory.url+'execute',{headers:factory.preview.headers},{headers:{path:factory.path,filetype:factory.fileType,filename:item.pathSuffix,indexName:factory.preview.title}}).then(function(res){
        item.$executing = false;
      });
    }
  };

  factory.preview = function(item){
    return $http.get(factory.url+'preview',{headers:{path:factory.path,filetype:factory.fileType,filename:item.pathSuffix}});
  };

  return factory;

}).factory('s3',function($http){
  var factory = {};
  factory.url = _CONFIG.proxyUrl+'/s3/';
  factory.fileTypes = [
    {
      name:'AGOL Logs',
      type:'agol_log'
    },
    {
      name:'S3 Logs',
      type:'s3_log'
    }
  ];

  factory.ingest = function(item){
    item.$executing = true;
    $http.get(factory.url+'execute',{headers:{type:item.type}}).then(function(res){
      item.$executing = true;
    });
  };

  return factory;

}).factory('streaming',function($http,$interval,$rootScope){
  var factory = {};
  factory.url = _CONFIG.url+'streaming';
  factory.topic = '';
  factory.topics = [];

  factory.addTopic = function(){
    factory.topics.push({name:factory.topic,data:[],time:0});
    $rootScope.reset(factory,['topic']);
  };

  factory.removeTopic = function(index){
    factory.topics.splice(index,1);
  };

  factory.poll = function(t){
    return $http.get(factory.url+'/'+t.name+'/'+t.time).then(function(res){
      angular.copy(res.data,t.data);
    });
    return res;
  };

  factory.send = function(t){
    return $http.post(factory.url+'/'+t.name,t.message).then(function(res){
      t.message = '';
      return res;
    });
  };

  return factory;

}).controller('ingest',function($scope,hdfs,s3,streaming){
  $scope.hdfs = hdfs;
  $scope.s3 = s3;
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