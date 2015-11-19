var _CONFIG = {
	url: 'http://localhost:8080/vandl-webapp/',
	fileSize:{
		groups:['B','KB','MB','GB','TB']
	},
	modules:[
		{
			label:'Extract',
			href:'#/extract',
			path:'extract.html'
		},
		{
			label:'Ingest',
			href:'#/ingest',
			path:'ingest.html'
		},
		{
			label:'Visualize',
			href:'#/visualize',
			path:'visualize.html'
		}
	]
};
_CONFIG.view = _CONFIG.modules[0].label;