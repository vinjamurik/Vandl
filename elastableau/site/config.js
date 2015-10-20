var _CONFIG = {
	proxyUrl: 'http://localhost:9001/proxy',
	ip:'http://172.31.22.52',
	fileSize:{
		groups:['B','KB','MB','GB','TB']
	},
	loginModule:{
		label:'Login',
		path:'login.html'
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
}