angular.module('elastableau',['ui.bootstrap','ngFileUpload']).controller('home',function($scope){
  $scope.view = 'extract';
}).factory('elastic',function($http){
	var factory = {};
  factory.index = '';
  factory.indices = [];
  factory.type = '';
  factory.types = {};
  factory.typeMap = {
    'long':'int',
    'double':'float'
  };
  factory.from = 0;
  factory.limit = 10000;
  factory.url = _CONFIG.proxyUrl+'/elastic/';
  factory.pData = [];
  factory.previewMode = false;

  factory.getIndices = function(){
    $http.get(factory.url+'indices').then(function(res){
      angular.forEach(res.data,function(v,k){
        factory.indices.push(k);
      });
    });
  };
  factory.getIndices();

  factory.resetPreview = function(){
    factory.previewMode = false;
    angular.copy([],factory.pData);
  };

  factory.getTypes = function(){
    factory.resetPreview();
    return $http.get(factory.url+factory.index+'/types').then(function(res){
      angular.copy({},factory.types);
      angular.forEach(res.data[factory.index].mappings,function(obj,typeName){
        factory.types[typeName] = {headers:{}};
        angular.copy(res.data[factory.index].mappings[typeName].properties,factory.types[typeName].headers);
      });
    });
  };

  return factory;

}).controller('extract',function($scope,$http,elastic){
	$scope.elastic = elastic;
	$scope.connector = tableau.makeConnector();

  $scope.connector.getColumnHeaders = function(){
  	$http.get(elastic.url+'headers').then(function(res){
  		tableau.headersCallback(res.data.names,res.data.types);
  	});
  };

  $scope.connector.getTableData = function(from){
    from = (from || 0);
    var config = {params:{from:from}};
    $http.get(elastic.url+'data',config).then(function(response){
      var data = [];
      angular.forEach(response.data.hits.hits,function(hit){
        data.push(hit._source);
      });
      var offset = +(from)+data.length;
      tableau.dataCallback(data,offset.toString(),data.length > 0);
    });
  };

  tableau.connectionName = 'Elastableau';
  tableau.registerConnector($scope.connector);

  $scope.preview = function(){
    if(elastic.previewMode){
      elastic.resetPreview();
    }else{
      elastic.previewMode = true;
      $http.get(elastic.url+'preview',{params:{index:elastic.index,type:elastic.type}}).then(function(res){
        angular.copy([],elastic.pData);
        angular.forEach(res.data.hits.hits,function(hit){
          elastic.pData.push(hit._source);
        });
      });
    }
  };

  $scope.submit = function(){
  	var body = {index:elastic.index,type:elastic.type,from:elastic.from,limit:elastic.limit,random:elastic.random};
  	body.headers = {names:[],types:[]};
  	angular.forEach(elastic.types[elastic.type].headers,function(typeObj,name){
  		body.headers.names.push(name);
  		body.headers.types.push(elastic.typeMap[typeObj.type] || typeObj.type);
  	});
    $http.post(elastic.url,body).then(function(){
      tableau.submit();
    });
  };

}).factory('hdfs',function(Upload,$http){
  var factory = {};
  factory.url = _CONFIG.proxyUrl+'/hdfs/';
  factory.dirFiles = [];
  factory.uploadFiles = [];
  factory.path = _CONFIG.defaultPath;
  factory.lastGoodPath = factory.path;
  factory.currentFolder = '';

  factory.getHumanizedSize = function(size,dPlaces){
    var g = 0;
    while(size/Math.pow(1000,g) > 1000){
      g++;
    }
    dPlaces = Math.pow(10,dPlaces);
    size /= Math.pow(1000,g);
    return Math.round(size*dPlaces)/dPlaces+' '+_CONFIG.fileSize.groups[g];
  };

  factory.goIntoFolder = function(folder){
    factory.currentFolder = folder;
    factory.path = factory.lastGoodPath+factory.currentFolder+'/';
    factory.on.pathChange();
  };

  factory.getOutOfFolder = function(){
    factory.path = factory.lastGoodPath.substring(0,factory.lastGoodPath.lastIndexOf(factory.currentFolder+'/'));
    factory.currentFolder = factory.path.length - 1 ? factory.path.match(/.*\/(.*)\//)[1] : '';
    factory.on.pathChange();
  };

  factory.on = {
    fileChange:function(){
      angular.forEach(factory.uploadFiles,function(f){
        f.humanSize = factory.getHumanizedSize(f.size,3);
      });
    },
    removeItem:function($index){
      factory.uploadFiles.splice($index,1);
    },
    pathChange:function(){
      factory.path = factory.path || _CONFIG.defaultPath;
      $http.get(factory.url+'dirStatus',{headers:{'path':factory.path}}).then(function(response){
        factory.showFolderOut = factory.path.indexOf('/') + 1;
        angular.copy([],factory.dirFiles);
        if(response.data.FileStatuses){
          factory.lastGoodPath = factory.path;
          angular.forEach(response.data.FileStatuses.FileStatus,function(item){
            item.humanSize = !!item.length ? factory.getHumanizedSize(item.length,3) : '0 '+_CONFIG.fileSize.groups[0];
            factory.dirFiles.push(item);
          });
        }
      });
    }
  };
  factory.on.pathChange();

  factory.upload = function(){
    Upload.upload({
      url:factory.url+'upload',
      file:factory.uploadFiles,
      headers:{'path':factory.path}
    }).progress(function(evt){

    }).success(function(){
      angular.copy([],factory.uploadFiles);
      factory.on.pathChange();
    });
  };

  factory.ingest = function(item){
    item.$executing = true;
    $http.get(factory.url+'execute',{headers:{path:factory.path+item.pathSuffix}}).then(function(res){
      item.$executing = false;
    });
  };

  return factory;

}).controller('ingest',function($scope,hdfs){
  $scope.hdfs = hdfs;
});