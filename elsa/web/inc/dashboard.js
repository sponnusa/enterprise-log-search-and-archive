function loadCharts(){
	// Callback that creates and populates a data table,
	// instantiates the pie chart, passes in the data and
	// draws it.
	YAHOO.ELSA.timeTypes = {
		timestamp:1,
		minute:1,
		hour:1,
		day:1,
		year:1
	};
	
	YAHOO.ELSA.chartOptions = {
		PieChart: {
			chartArea: {
				height: '75%',
				width: '75%'
			}
		},
		BarChart: {
			vAxis: {
				textPosition: 'in'
			}
		},
		ColumnChart: {
			
		},
		AreaChart: {},
		Table: {}
	}
	
	var oElCharts = document.getElementById('google_charts');
	
	for (var i in YAHOO.ELSA.dashboardRows){
		var oElDiv = document.createElement('div');
		oElCharts.appendChild(oElDiv);
		if (YAHOO.ELSA.dashboardRows[i].title){
			var oElRowTitle = document.createElement('h2');
			oElRowTitle.innerText = YAHOO.ELSA.dashboardRows[i].title;
			oElDiv.appendChild(oElRowTitle);
			oElRowTitle = new YAHOO.util.Element(oElRowTitle);
			//oElTitle.setStyle('text-align', 'center');
			oElRowTitle.addClass('chart_title');
		}
		
		var oElTr;
		if (YAHOO.ELSA.dashboardRows[i].charts.length > 1){
			var oElTable = document.createElement('table');
			//oElTable.width = '100%';
			oElDiv.appendChild(oElTable);
			var oElTbody = document.createElement('tbody');
			oElTable.appendChild(oElTbody);
			oElTr = document.createElement('tr');
			oElTbody.appendChild(oElTr);
		}
		else {
			oElTr = null;
		}
		
		for (var j in YAHOO.ELSA.dashboardRows[i].charts){
			var oChart = YAHOO.ELSA.dashboardRows[i].charts[j];
			var oTargetEl;
			if (oElTr){
				var oElTd = document.createElement('td');
				oElTr.appendChild(oElTd);
				oTargetEl = oElTd;
			}
			else {
				oTargetEl = oElDiv;
			}
								
			var aNeededIds = [ 'chart', 'dashboard', 'control' ];
			for (var k in aNeededIds){
				var sType = aNeededIds[k];
				var sId = sType + '_' + i + '_' + j;
				var oEl = document.createElement('div');
				oEl.id = sId;
				oTargetEl.appendChild(oEl);
				//oElDiv.appendChild(oEl);
				oChart[sType + '_el'] = oEl;
			}
			
			// Counter to see when we've gotten all of our async query results
			oChart.received = 0;
						
			for (var k in oChart.queries){
				sendQuery(i, j, k);
			}
		}
	}
}

function sendQuery(p_iRowId, p_iChartId, p_iQueryId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
	var oQuery = oChart.queries[p_iQueryId];
	var sReqId = p_iRowId + '_' + p_iChartId + '_' + p_iQueryId;
	var oDSQuery = new google.visualization.Query('/datasource/?tqx=reqId:' + sReqId
		+ ';out:json&q=' + encodeURIComponent(JSON.stringify(oQuery)));
	var fnStoreResult = function(p_oResponse){
		if (p_oResponse.isError()){
			logger.log('FAIL: ' + p_oResponse.getMessage() + ' ' + p_oResponse.getDetailedMessage());
			return;
		}
		
		// Check to see if any of the queries are time-based
		var sTime = false;
		for (var i in oChart.queries){
			var oIndividualQuery = oChart.queries[i];
			if (oIndividualQuery.query_meta_params.groupby){
				logger.log('oIndividualQuery.groupby', oIndividualQuery.query_meta_params.groupby[0]);
				if (YAHOO.ELSA.timeTypes[ oIndividualQuery.query_meta_params.groupby[0] ]){
					sTime = oIndividualQuery.query_meta_params.groupby[0];
					logger.log('set sTime to true because found group ' + oIndividualQuery.query_meta_params.groupby[0]);
					break;
				}
			}
		}
		oChart.isTimeChart = sTime;
		
		if (oChart.dataTable){
			mergeDataTables(oChart.dataTable, p_oResponse.getDataTable(), oQuery.label, oChart.isTimeChart);
		}
		else {
			oChart.dataTable = p_oResponse.getDataTable();
		}
		
		if (oChart.group){
			if (typeof(google.visualization.data[oChart.group]) != 'undefined'){
				oChart.dataTable = google.visualization.data.group(oChart.dataTable, [0], 
				[{'column': 1, 'aggregation': google.visualization.data[oChart.group], 'type': 'number'}]);
			}
			else {
				logger.log('invalid group: ' + oChart.group);
			}
		}
		
		oChart.received++;
		if (oChart.received == oChart.queries.length){
			logger.log('received all (' + oChart.queries.length + ') with query id ' + p_iQueryId + ' chart data for chart ' + p_iChartId);
			logger.log('query: ' + oQuery.query_string);
			try {
				drawChart(p_iRowId, p_iChartId);
			} catch (e){ logger.log('error drawing chart', e); }
		}
	}
	oDSQuery.send(fnStoreResult);
}

function mergeDataTables(p_oBaseTable, p_oAddTable, p_sLabel, p_bIsTime){
	p_oBaseTable.addColumn('number', p_sLabel);
	try {
		var iNumAdded = 0;
		var iNumCols = p_oBaseTable.getNumberOfColumns();
		// For each time value in our add table, add to the appropriate bucket in the existing table
		for (var i = 0; i < p_oAddTable.getNumberOfRows(); i++){
			var x = p_oAddTable.getValue(i, 0);
			var y = p_oAddTable.getValue(i, 1);
			
			var aRowsForUpdate;
			//if (p_bIsTime){
			//	aRowsForUpdate = p_oBaseTable.getFilteredRows([{ column:0, minValue:x, maxValue:new Date(x.getTime() + 1000)}]);
			//}
			//else {
				aRowsForUpdate = p_oBaseTable.getFilteredRows([{ column:0, value:x}]);
			//}
			if (aRowsForUpdate.length){
				p_oBaseTable.setCell(aRowsForUpdate[0], (iNumCols - 1), y);
				//logger.log('set cell ' + aRowsForUpdate[0] + ' ' + (iNumCols - 1) + ' ' + y);
			}
			else {
				//logger.log('no date for ', oDate);
				var aNewRow = [x];
				for (var j = 1; j < (iNumCols - 1); j++){
					aNewRow.push(null);
				}
				aNewRow.push(y);
				p_oBaseTable.addRow(aNewRow);
				iNumAdded++;
			}
		}
		if (iNumAdded){
			p_oBaseTable.sort({column:0});
		}
	} catch(e){ logger.log('Error merging tables', e); }
}


function drawChart(p_iRowId, p_iChartId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
	oChart.dataTable.setColumnLabel(1, oChart.queries[0].query_meta_params.comment);
		
	if (oChart.isTimeChart){
		makeTimeChart(p_iRowId, p_iChartId);
	}
	else if (oChart.type == 'GeoChart'){
		makeGeoChart(p_iRowId, p_iChartId);
	}
	else {
		makeSimpleChart(p_iRowId, p_iChartId);
	}
}

function makeSimpleChart(p_iRowId, p_iChartId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
	
	var oOptions = YAHOO.ELSA.chartOptions[oChart.type];
	oOptions.title = oChart.title;
	oChart.wrapper = new google.visualization.ChartWrapper({
		dataTable: oChart.dataTable,
		containerId: oChart.chart_el,
		chartType: oChart.type,
		options: oOptions
	});
	
	google.visualization.events.addListener(oChart.wrapper, 'ready', function(){
		google.visualization.events.addListener(oChart.wrapper.getChart(), 'select', function(){ selectHandler(p_iRowId, p_iChartId) });
	});
	oChart.wrapper.draw();
}

function makeGeoChart(p_iRowId, p_iChartId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
		
	oChart.wrapper = new google.visualization.ChartWrapper({
		dataTable: oChart.dataTable,
		containerId: oChart.chart_el,
		chartType: oChart.type,
		options: { title: oChart.title }
	});
	
	google.visualization.events.addListener(oChart.wrapper, 'ready', function(){
		google.visualization.events.addListener(oChart.wrapper.getChart(), 'regionClick', function(){ selectHandler(p_iRowId, p_iChartId) });
	});
	oChart.wrapper.draw();
}

function makeChart(p_iRowId, p_iChartId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
	oChart.dashboard = new google.visualization.Dashboard(YAHOO.util.Dom.get(oChart.dashboard_el));
	
	var oRange = oChart.dataTable.getColumnRange(0);
	logger.log('oRange', oRange);
	logger.log('oRange', oRange.min);
	logger.log('oRange', oRange.max);
	logger.log('range diff: ' + (oRange.max - oRange.min));
	var iStep = (oRange.max - oRange.min) / 10;
	logger.log('iStep ' + iStep);
	var oFirstStep = oRange.min + iStep;
	
	var aChartCols = [];
	for (var i = 0; i < oChart.dataTable.getNumberOfColumns(); i++){
		aChartCols.push(i);
	}
	
	oChart.control = new google.visualization.ControlWrapper({
		'controlType': 'ChartRangeFilter',
		'containerId': oChart.control_el,
		'options': {
			// Filter by the date axis.
			'filterColumnIndex': 0,
			'ui': {
				'chartType': 'LineChart',
				'chartOptions': {
					'chartArea': {'width': '90%'},
					'hAxis': {'baselineColor': 'none'}
				},
				'chartView': {
					'columns': aChartCols
				},
				'minRangeSize': iStep
			}
     	},
		'state': {'range': {'start': oRange.min, 'end': oFirstStep}}
	});
	
	var bIsStacked = false;
	if (oChart.isStacked){
		bIsStacked = true;
	}
	oChart.wrapper = new google.visualization.ChartWrapper({
		dataTable: oChart.dataTable,
		containerId: oChart.chart_el,
		chartType: oChart.type,
		options: { 'title': oChart.title, isStacked: bIsStacked }
	});
		
	google.visualization.events.addListener(oChart.wrapper, 'ready', function(){
		google.visualization.events.addListener(oChart.wrapper.getChart(), 'regionClick', function(){ selectHandler(p_iRowId, p_iChartId) });
	});
	oChart.dashboard.bind(oChart.control, oChart.wrapper);
	oChart.dashboard.draw(oChart.dataTable);
}

function makeTimeChart(p_iRowId, p_iChartId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
	oChart.dashboard = new google.visualization.Dashboard(YAHOO.util.Dom.get(oChart.dashboard_el));
	var oRange = oChart.dataTable.getColumnRange(0);
	logger.log('oRange', oRange);
	logger.log('oRange', oRange.min);
	logger.log('oRange', oRange.max);
	logger.log('range diff: ' + (oRange.max.getTime() - oRange.min.getTime()));
	var iStep = (oRange.max.getTime() - oRange.min.getTime()) / 10;
	logger.log('iStep ' + iStep);
	var oFirstStep = new Date(oRange.min.getTime() + iStep);
		
	var aChartCols = [];
	for (var i = 0; i < oChart.dataTable.getNumberOfColumns(); i++){
		aChartCols.push(i);
	}
	oChart.control = new google.visualization.ControlWrapper({
		'controlType': 'ChartRangeFilter',
		'containerId': oChart.control_el,
		'options': {
			// Filter by the date axis.
			'filterColumnIndex': 0,
			'ui': {
				'chartType': 'LineChart',
				'chartOptions': {
					'chartArea': {'width': '90%'},
					'hAxis': {'baselineColor': 'none'}
				},
				'chartView': {
					'columns': aChartCols
				},
				'minRangeSize': iStep
			}
     	},
		'state': {'range': {'start': oRange.min, 'end': oFirstStep}}
	});
				
	var bIsStacked = false;
	if (oChart.isStacked){
		bIsStacked = true;
	}
	oChart.wrapper = new google.visualization.ChartWrapper({
		dataTable: oChart.dataTable,
		containerId: oChart.chart_el,
		chartType: oChart.type,
		options: { 'title': oChart.title, isStacked: bIsStacked }
	});
	
	google.visualization.events.addListener(oChart.wrapper, 'ready', function(){
		google.visualization.events.addListener(oChart.wrapper.getChart(), 'select', function(){ selectHandler(p_iRowId, p_iChartId) });
	});
			
	var openEditor = function(){
		logger.log(google.visualization.ChartEditor);
		// Handler for the "Open Editor" button.
		var editor = new google.visualization.ChartEditor();
		google.visualization.events.addListener(editor, 'ok',
		function() {
			oChart.wrapper = editor.getChartWrapper();
			oChart.wrapper.draw(document.getElementById(chartEl.id));
		});
		editor.openDialog(oChart.wrapper);
	}
	//var oButtonEl = document.createElement('button');
	//oButtonEl.id = 'open_editor_' + p_iId;
	var oButton = new YAHOO.widget.Button({
		container: 'charts',
		id: 'open_editor_' + p_iRowId + '_' + p_iChartId,
		type: 'button',
		label: 'Edit',
		name: 'open_editor_',
		onclick: { 	
			fn: openEditor,
			scope: YAHOO.ELSA,
			correctScope: false
		}
	});
	oChart.dashboard.bind(oChart.control, oChart.wrapper);
	oChart.dashboard.draw(oChart.dataTable);
}

var selectHandler = function(p_iRowId, p_iChartId){
	var oChart = YAHOO.ELSA.dashboardRows[p_iRowId].charts[p_iChartId];
	var oSelection = oChart.wrapper.getChart().getSelection();
	var oDataTable = oChart.wrapper.getDataTable();
	logger.log('select', oSelection);
			
	var message = '';
	  for (var i = 0; i < oSelection.length; i++) {
	    var item = oSelection[i];
	    if (item.row != null && item.column != null) {
	      var str = oDataTable.getFormattedValue(item.row, item.column);
	      message += '{row:' + item.row + ',column:' + item.column + '} = ' + str;
	      logger.log(oDataTable.getColumnLabel(item.column));
	      logger.log(oDataTable.getColumnProperties(item.column));
	      logger.log(oDataTable.getColumnProperty(item.column, 'value'));
	      logger.log(oDataTable.getProperties(item.row, item.column));
	    } else if (item.row != null) {
	      var str = oDataTable.getFormattedValue(item.row, 0);
	      message += '{row:' + item.row + ', (no column, showing first)} = ' + str;
	    } else if (item.column != null) {
	      var str = oDataTable.getFormattedValue(0, item.column);
	      message += '{(no row, showing first), column:' + item.column + '} = ' + str;
	    }
	  }
	  if (message == '') {
	    message = 'nothing';
	  }
	  logger.log(message);
}
