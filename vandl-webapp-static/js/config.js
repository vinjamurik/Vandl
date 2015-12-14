var _CONFIG = {
	url: 'https://localhost:8443/vandl-webapp/',
	fileSize:{
		groups:['B','KB','MB','GB','TB']
	},
	modules:[
		{
			label:'Extract',
			href:'#/extract',
			path:'html/extract.html'
		},
		{
			label:'Ingest',
			href:'#/ingest',
			path:'html/ingest.html'
		},
		{
			label:'Visualize',
			href:'#/visualize',
			path:'html/visualize.html'
		}
	]
};
_CONFIG.view = _CONFIG.modules[0].label;