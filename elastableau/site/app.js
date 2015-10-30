angular.module('elastableau',['ui.bootstrap','ngFileUpload'])
.controller('home',function($scope){$scope._CONFIG = _CONFIG;})
.factory('elastic',function($http){
	var factory = {};
  factory.index = '';
  factory.indices = [];
  factory.type = '';
  factory.types = {};
  factory.from = 0;
  factory.limit = 0;
  factory.random = false;
  factory.url = _CONFIG.proxyUrl+'/elastic/';
  factory.data = [];
  factory.count = 0;
  factory.connector = tableau.makeConnector();

  factory.reset = function(args){
    angular.forEach(args,function(arg){
      switch(typeof factory[arg]){
        case 'object':
          factory[arg] = angular.isArray(factory[arg]) ? [] : {};
          break;
        case 'string':
          factory[arg] = '';
          break;
        default:
          factory[arg] = 0;
      }
    });
  };
  
  factory.getIndices = function(){
    factory.reset(['index','indices']);
    $http.get(factory.url+'indices').then(function(res){
      angular.forEach(angular.extend({'':{}},res.data),function(v,k){
        factory.indices.push(k);
      });
    });
  };
  factory.getIndices();

  factory.getTypes = function(){
    factory.reset(['type','types','count']);
    if(factory.index){
      return $http.get(factory.url+'types',{params:{index:factory.index}}).then(function(res){
        angular.forEach(angular.extend({'':{}},res.data[factory.index].mappings),function(typeVal,type){
          factory.types[type] = [];
          angular.forEach(typeVal.properties,function(obj,key){
            factory.types[type].push({name:key,type:obj.type});
          });
        });
      });
    }
  };

  factory.connector.getColumnHeaders = function(){
    var headers = {names:[],types:[]};
    angular.forEach(JSON.parse(tableau.connectionData).fields,function(field){
      headers.names.push(field.name);
      headers.types.push(field.type);
    });
    tableau.headersCallback(headers.names,headers.types);
  };

  factory.connector.getTableData = function(from){
    var params = JSON.parse(tableau.connectionData);
    from = from || params.from;
    delete params.fields;
    params.from = from;
    $http.get(factory.url+'data',{params:params}).then(function(res){
      var data = [];
      angular.forEach(res.data.hits.hits,function(hit){
        data.push(hit._source);
      });
      var moreRecords = data.length > 0;
      if(params.limit > 0){
        from = +from+data.length;
      }else{
        if(!from){
          moreRecords = true;
        }
        from = res.data._scroll_id;
      }
      tableau.dataCallback(data,from,moreRecords);
    });
  };

  factory.preview = function(){
    factory.reset(['count','data']);
    if(factory.type){
      $http.get(factory.url+'preview',{params:{index:factory.index,type:factory.type}}).then(function(res){
        factory.count = res.data.hits.total;
        factory.data = res.data.hits.hits;
      });
    }
  };

  factory.extract = function(){
    factory.from = +!factory.random*factory.from;
    tableau.connectionData = JSON.stringify({index:factory.index,type:factory.type,fields:factory.types[factory.type],from:factory.from,limit:factory.limit,random:factory.random});
    tableau.submit();
  };

  factory.delete = function(){
    $http.delete(factory.url+'delete',{params:{index:factory.index,type:factory.type}}).then(function(res){
      factory.reset(['type','types']);
      factory.getIndices();
    });
  };

  tableau.connectionName = 'Elastableau';
  tableau.registerConnector(factory.connector);

  return factory;

}).controller('extract',function($scope,elastic){
	$scope.elastic = elastic;
}).factory('hdfs',function(Upload,$http){
  var factory = {};
  factory.url = _CONFIG.proxyUrl+'/hdfs/';
  factory.dirFiles = [];
  factory.uploadFiles = [];
  factory.path = '/';
  factory.currentFolder = '';
  factory.fileType = '';
  factory.uploadDisabled = true;

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
  factory.on.pathChange();

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

  factory.ingest = function(item){
    item.$executing = true;
    $http.get(factory.url+'execute',{headers:{path:factory.path,filetype:factory.fileType,filename:item.pathSuffix}}).then(function(res){
      item.$executing = false;
    });
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

}).controller('ingest',function($scope,hdfs,s3){
  $scope.hdfs = hdfs;
  $scope.s3 = s3;
}).factory('visualize',function(){
  var factory = {};
  factory.images = [
    {
      img:'img/kibana.png',
      href:'http://172.31.84.221:5601'
    },
    {
      img:'img/plotly.png',
      href:''
    },
    {
      img:'img/tableau.png',
      href:'http://172.31.9.66'
    },
    {
      img:'img/bokeh.png',
      href:'http://172.31.48.155'
    },
    {
      img:'img/rshiny.png',
      href:''
    },
    {
      img:'img/paxata.png',
      href:'https://pri-poc.paxata.com'
    }
  ];

  return factory;

}).controller('visualize',function(visualize,$scope){
  $scope.visualize = visualize;
});