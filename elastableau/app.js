angular.module('elastableau',['ui.bootstrap','ngFileUpload']).controller('home',function($scope){
  $scope.view = 'extract';
}).factory('elastic',function($http,$httpParamSerializer){
	var factory = {};
  factory.index = '';
  factory.indices = [];
  factory.type = '';
  factory.types = {};
  factory.from = 0;
  factory.limit = 0;
  factory.random = false;
  factory.url = _CONFIG.proxyUrl+'/elastic/';
  factory.qUrl = '';
  factory.pData = [];
  factory.pCount = 0;
  factory.previewMode = false;

  factory.getIndices = function(){
    $http.get(factory.url+'indices').then(function(res){
      angular.copy([],factory.indices);
      angular.forEach(res.data,function(v,k){
        factory.indices.push(k);
      });
    });
  };

  factory.resetPreview = function(){
    factory.previewMode = false;
    angular.copy([],factory.pData);
  };

  factory.getTypes = function(){
    factory.resetPreview();
    return $http.get(factory.url+factory.index+'/types').then(function(res){
      factory.types = res.data;
    });
  };

  factory.generateQueryMap = function(){
    var params = window.location.search.split('&');
    var queryMap = {};
    if(params.length && params[0].charAt(0) == '?'){
      params[0] = params[0].substring(1);
    }
    angular.forEach(params,function(item){
      var kv = item.split('=');
      queryMap[kv[0]] = kv[1];
    });
    angular.extend(factory,queryMap);
  };

  factory.generateQUrl = function(){
    var params = {index:factory.index,type:factory.type,from:factory.from,limit:factory.limit,random:factory.random};
    factory.qUrl = _CONFIG.Ip+':'+window.location.port+window.location.pathname+'?'+$httpParamSerializer(params);
    return factory.qUrl;
  };

  return factory;

}).controller('extract',function($scope,$http,elastic){
	$scope.elastic = elastic;
	$scope.connector = tableau.makeConnector();
  elastic.getIndices();

  $scope.connector.getColumnHeaders = function(){
  	$http.get(elastic.url+'headers').then(function(res){
  		tableau.headersCallback(res.data.names,res.data.types);
  	});
  };

  $scope.connector.getTableData = function(from){
    from = (from || 0);
    var config = {params:{}};
    if(elastic.limit > 0){
      config.params.from = from;
    }else{
      config.params.scroll_id = from;
    }
    $http.get(elastic.url+'data',config).then(function(response){
      var data = [];
      angular.forEach(response.data.hits.hits,function(hit){
        data.push(hit._source);
      });
      if(elastic.limit > 0){
        var offset = +(from)+data.length;
        tableau.dataCallback(data,offset.toString(),data.length > 0);
      }else{
        tableau.dataCallback(data,response.data._scroll_id,data.length > 0);
      }
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
        elastic.pCount = res.data.hits.total;
        angular.copy([],elastic.pData);
        angular.forEach(res.data.hits.hits,function(hit){
          elastic.pData.push(hit._source);
        });
      });
    }
  };

  $scope.connect = function(){
  	window.location.href = elastic.generateQUrl();
  };

  $scope.submit = function(){
    elastic.generateQueryMap();
    if(elastic.index){
      elastic.getTypes().then(function(){
        var params = {type:elastic.type,from:elastic.from,limit:elastic.limit,random:elastic.random};
        $http.get(elastic.url,{params:params}).then(function(){
          tableau.submit();
        });
      });
    }
  };
  $scope.submit();

}).factory('hdfs',function(Upload,$http){
  var factory = {};
  factory.url = _CONFIG.proxyUrl+'/hdfs/';
  factory.dirFiles = [];
  factory.uploadFiles = [];
  factory.path = _CONFIG.defaultPath;
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
      factory.path = factory.path || _CONFIG.defaultPath;
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

}).controller('ingest',function($scope,hdfs){
  $scope.hdfs = hdfs;
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
    }
  ];

  return factory;
}).controller('visualize',function(visualize,$scope){
  $scope.visualize = visualize;
});