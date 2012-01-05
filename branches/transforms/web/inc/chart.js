YAHOO.namespace('YAHOO.ELSA.Chart');
YAHOO.ELSA.Charts = [];
YAHOO.ELSA.Chart = function(){};
YAHOO.ELSA.Chart.registeredCallbacks = {};

YAHOO.ELSA.Chart.open_flash_chart_data = function(p_iId){
	try {
		logger.log('returning chart data for id:' + p_iId, YAHOO.ELSA.Charts[p_iId]);
		return YAHOO.lang.JSON.stringify(YAHOO.ELSA.Charts[p_iId].cfg);
	}
	catch (e){
		logger.log('exception: ' + e);
	}
}

// Auto-graph given a graph type, title, and AoH of data
YAHOO.ELSA.Chart.Auto = function(p_oElContainerId, p_sType, p_sTitle, p_oData, p_callback){
	if (typeof p_callback == 'undefined'){
		p_callback = function(){};
	}
	YAHOO.ELSA.Chart.registeredCallbacks[p_oElContainerId] = p_callback;
	logger.log('given container id: ' + p_oElContainerId);
	var id = YAHOO.ELSA.Charts.length;
	this.id = id;
	this.colorPalette = [
		'#FF0000',
		'#FFFF00',
		'#00FF00',
		'#FF0080',
		'#FF00FF',
		'#0000FF'
	];
	
	this.type = p_sType;
	// Scrub nulls
	this.ymax = 0;
	for (var i in p_oData){
		if (i.match(/^x/)){
			continue;
		}
		var max = 0;
		logger.log('i: ' + i);
		for (var j = 0; j < p_oData[i].length; j++){
			//logger.log('typeof:', typeof p_oData[i][j]);
			if (p_oData[i][j] && typeof p_oData[i][j] === 'object'){
				logger.log('p_oData[i][j]', p_oData[i][j]);
				if (typeof p_oData[i][j]['val'] == 'undefined'){
					for (var k in p_oData[i][j]){
						logger.log('k: ' + k + ', data: ' + p_oData[i][j][k] + ', ymax: ' + this.ymax);
						if (parseInt(p_oData[i][j][k]) > this.ymax){
							this.ymax = parseInt(p_oData[i][j][k]);
						}
					}
				}
				else {
					p_oData[i][j]['val'] = parseInt(p_oData[i][j]['val']);
					if (this.ymax < p_oData[i][j]['val']){
						this.ymax = p_oData[i][j]['val'];
					}
				}
			}
			else {
				var tmp = parseInt(p_oData[i][j]);
				if (tmp && typeof tmp != 'Object'){
					p_oData[i][j] = tmp;
				}
				else {
					p_oData[i][j] = 0;
				}
				if (this.ymax < p_oData[i][j]){
					this.ymax = p_oData[i][j];
				}
			}
		}
		if (max > this.ymax){
			this.ymax = max;
		}
	}
	logger.log('max:' + this.ymax);
	
	// Figure out columns using the first row
	var aElements = [];
	var iCounter = 0;
	var iColorPaletteLength = this.colorPalette.length;
	for (var key in p_oData){
		if (key == 'x'){
			continue;
		}
		var aValues = [];
		for (var i in p_oData[key]){
			var val = p_oData[key][i];
			if (typeof val == 'object'){
				var iSum = 0;
				for (var j in p_oData[key][i]){
					if (j == 'val'){
						continue;
					}
					logger.log('iSum: ' + iSum + ', j: ' + j + ', val: ' + p_oData[key][i][j]);
					iSum = iSum + parseInt(p_oData[key][i][j]);
				}
				aValues.push({
					top:iSum, 
					label:p_oData.x[i], 
					tip:key + ' ' + p_oData.x[i] + '<br>#val#', 
					'on-click': 'function(){ YAHOO.ELSA.Chart.registeredCallbacks[\'' + p_oElContainerId + '\'](' + this.id + ', ' + i + ')}'
				});
			}
			else {
				aValues.push({
					top:val, 
					label:p_oData.x[i], 
					tip:key + ' ' + p_oData.x[i] + '<br>#val#', 
					'on-click': 'function(){ YAHOO.ELSA.Chart.registeredCallbacks[\'' + p_oElContainerId + '\'](' + this.id + ', ' + i + ')}'
				});
			}
		}
		aElements.push({
			type:p_sType,
			colour: this.colorPalette[((iColorPaletteLength - (iCounter % iColorPaletteLength)) - 1)],
			text: key,
			values: aValues
		});
		iCounter++;
	}
	
	// calculate label steps
	var iXLabelSteps = 1;
	if (p_oData.x.length > 10){
		iXLabelSteps = parseInt(p_oData.x.length / 10);
	}
	var aLabels = [];
	for (var i = 0; i < p_oData.x.length; i += iXLabelSteps){
		aLabels.push(p_oData.x[i]);
	}
	
	var chartCfg = {
		title: {
			text:unescape(p_sTitle),
			style:'{font-size:16px;}' 
		},
		elements: aElements,
		x_axis:{
			labels:{
				labels:p_oData.x,
				rotate:330,
				'visible-steps': iXLabelSteps
			}
		},
		y_axis:{
			max:this.ymax,
			steps:(this.ymax / 10)
		}
	}
	this.cfg = chartCfg;
	
	// create a div within the given container so we can append the "Save As..." link
	var outerContainerDiv = YAHOO.util.Dom.get(p_oElContainerId);
	var linkDiv = document.createElement('div');
	linkDiv.id = p_oElContainerId + '_link';
	var saveLink = document.createElement('a');
	saveLink.setAttribute('href', '#');
	saveLink.innerHTML = 'Save Chart As...';
	var aEl = new YAHOO.util.Element(saveLink);
	aEl.on('click', YAHOO.ELSA.Chart.saveImage, this.id);
	linkDiv.appendChild(saveLink);
	outerContainerDiv.appendChild(linkDiv);
	
	var containerDiv = document.createElement('div');
	containerDiv.id = p_oElContainerId + '_container';
	outerContainerDiv.appendChild(containerDiv);
	this.container = containerDiv.id;
	
	YAHOO.ELSA.Charts.push(this);
	logger.log('outerContainerDiv', outerContainerDiv);
	try {
		swfobject.embedSWF("inc/open-flash-chart.swf", this.container, 500, 300, 
			"9.0.0", "expressInstall.swf", 
			{"get-data" : "YAHOO.ELSA.Chart.open_flash_chart_data", "id" : this.id }, {"wmode" : "opaque"} );
	}
	catch (e){
		YAHOO.ELSA.Error(e);
	}
	logger.log('element: ', YAHOO.util.Dom.get(this.container));
};

YAHOO.ELSA.Chart.saveImage = function (p_oEvent, p_iId){
	logger.log('save image with id ' + p_iId);
	try {
		var sImageData = YAHOO.util.Dom.get(YAHOO.ELSA.Charts[p_iId].container).get_img_binary();
		var oEl = document.createElement('img');
		oEl.id = 'save_image';
		oEl.src = 'data:image/png;base64,' + sImageData;
		win = window.open('', 'SaveChart', 'left=20,top=20,width=700,height=500,toolbar=0,resizable=1,status=0');
		win.document.body.appendChild(oEl);
	}
	catch (e){
		YAHOO.ELSA.Error(e);
	}
}
