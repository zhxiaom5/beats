		console.log("Javascript engine loaded "); 
		process = function (fields){
			addfields={}
			addfields.javascript="zhxiaom5"
			console.log(JSON.stringify(fields))
//			var d = new Date(fields.timestamp)
//			console.log(" date " + d)
			return addfields
		}
