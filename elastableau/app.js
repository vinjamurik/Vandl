angular.module('elastableau',[]).factory('elastic',function($http){
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
    factory.url = 'http://localhost:9001/proxy/elastic/';
    factory.pData = [];
    factory.previewMode = false;

    factory.getIndices = function(){
      $http.get('http://localhost:9001/proxy/elastic/indices').then(function(res){
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

}).controller('elastableau',function($scope,$http,elastic){
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
});