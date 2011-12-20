YAHOO.namespace('YAHOO.ELSA');

YAHOO.ELSA.queryResultCounter = 0;
YAHOO.ELSA.localResults = [];
YAHOO.ELSA.viewMode = 'prod';
YAHOO.ELSA.panels = {};
YAHOO.ELSA.overlayManager = new YAHOO.widget.OverlayManager();
YAHOO.ELSA.logger = new Object;
YAHOO.ELSA.localGroupByQueryLimit = 1000; //number of recs to download locally and group by on
YAHOO.ELSA.Labels = {
	noTerm: 'Add Term',
	noGroupBy: 'None',
	defaultGroupBy: 'Report On',
	index: 'Index',
	archive: 'Archive'
}
YAHOO.ELSA.TimeTranslation = {
	Days: [ 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat' ],
	Months: [ 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec' ]
};

YAHOO.ELSA.initLogger = function(){
	/* Setup logging */
	if (YAHOO.ELSA.viewMode == 'dev'){
		YAHOO.widget.Logger.categories.push('elsa');
		logger = new YAHOO.ELSA.ConsoleProvider();
		var myLogReader = new YAHOO.widget.LogReader("logger");
		myLogReader.collapse();
		var tildeKeyListener = function(event){
			if(event && event.keyCode && event.keyCode==192){
				if(event.target && (event.target.nodeName=='INPUT' || event.target.nodeName=='TEXTAREA')){
					return true;
				}
				if(myLogReader.isCollapsed){
					myLogReader.expand();
				}else{
					myLogReader.collapse();
				}
			}
		}
		try{
			var w = new YAHOO.util.Element(document);
			w.subscribe("keyup", tildeKeyListener); 
		}catch(e){
			logger.log('Error subscribing document keyup', e);
		}
	}
	else {
		// Just create dummy logging functionality
		var fakeLogger = function(){};
		fakeLogger.prototype = {
		    log: function(msg, lvl) {
			}
		};
		logger = new fakeLogger();
	}
};

YAHOO.ELSA.getLocalResultId = function(p_oTab){
	// find the result that has this tabid
	var iTabIndex = YAHOO.ELSA.tabView.getTabIndex(p_oTab);
	for (var i in YAHOO.ELSA.localResults){
		if (typeof YAHOO.ELSA.localResults[i].tabId != 'undefined' && YAHOO.ELSA.localResults[i].tabId == iTabIndex){
			return i;
		}
	}
	logger.log('Unable to find local result for tab ' + iTabIndex);
}

YAHOO.ELSA.getLocalResultIdFromQueryId = function(p_iQid){
	for (var i in YAHOO.ELSA.localResults){
		if (typeof YAHOO.ELSA.localResults[i].id != 'undefined' && YAHOO.ELSA.localResults[i].id == p_iQid){
			return i;
		}
	}
	logger.log('Unable to find local result for qid ' + p_iQid);
}

YAHOO.ELSA.updateTabIds = function(p_iRemovedTabId){
	logger.log('updating tab ids');
	for (var i in YAHOO.ELSA.localResults){
		logger.log('id: ' + YAHOO.ELSA.localResults[i].id + ', tabid: ' + YAHOO.ELSA.localResults[i].tabId);
	}
	// decrement any results that had a tab id greater than or equal to the tab that was removed so that everything is synced
	for (var i in YAHOO.ELSA.localResults){
		if (YAHOO.ELSA.localResults[i].tabId >= p_iRemovedTabId){
			logger.log('decrementing tabId ' + YAHOO.ELSA.localResults[i].tabId);
			YAHOO.ELSA.localResults[i].tabId--;
		}	
	}
}

YAHOO.ELSA.LogWriter = function() {
    this.myLogReader = new YAHOO.widget.LogReader("logger");
    var lR = this.myLogReader;
    YAHOO.widget.Logger.log("My log message", 'error');
    //Modify the footer to container a search box.
    var logFt = this.myLogReader._elFt;
    //Add event handler that modifies _elConsole
    var keyupHandler = function(event){
    	var s = event.target;
    	if(!s || !s.value){
    		lR.resume();
    		return true;
    	}
    	lR.pause();//Keyup event shows up on top of the ones that we're hiding, so stop it for now.
    	var searchVal = s.value;
    	var logConsole = lR._elConsole;
    	var pres = logConsole.getElementsByTagName('pre');
    	//build regexp based on the given words to ignore extra spaces and what-not
    	var reArray = [];
    	var words = (searchVal && searchVal!='') ? searchVal.split(' '):[];
    	for(var w in words){
    		if(words[w]==' ' || words[w]==''){
    			continue;
    		}
    		var esc = words[w].replace(/([\\\/\.\*\?\+\-\^\$\(\)\{\}\[\]])/g, '\\$1');
    		reArray.push(esc);
    		
    	}
    	var re = new RegExp(RegExp.escape(reArray.join('\\s+')));
    	for(var p in pres){
    		if(typeof(pres[p])=='object' && pres[p]){
	    		if(!pres[p] || !pres[p].innerHTML){
	    			continue;
	    		}

	    		if(searchVal==''){//Nothing in searchVal
	    			YAHOO.util.Dom.removeClass(pres[p], 'hiddenElement');
	    			continue;
	    		}
	    		//take the innerHTML, strip out markup, then compare the resulting text
	    		var searchTxt = pres[p].innerHTML.replace(/<\/?\w.*?>/ig,'');
	    		//Replace HTML encoded entities with their equivalent string
	    		searchTxt = searchTxt.replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&').replace('&quot;', '"').replace("&apos;", "'");
	    		//Ideally, we would replace hex encoded values, too
	    		try{
	    			//Based on initial tests, it looks like this is useless, but I'll leave it in, anyway
		    		searchTxt = searchTxt.replace(/&#x([0-9A-Za-z]+);/ig, unescape('%$1'));
	    		}catch(e){
	    			logger.log(e+' occurred while removing hex-encoded values from the search string.');
	    		}
	    		if(!searchTxt.match(re)){
	    			YAHOO.util.Dom.addClass(pres[p], 'hiddenElement');
	    		}else{
	    			YAHOO.util.Dom.removeClass(pres[p], 'hiddenElement');
	    		}
    		}
    	}
    }
    //Create search box
    var sinput = document.createElement('input');
    var search = new YAHOO.util.Element(sinput);
    search.subscribe('keyup', keyupHandler);
    var sc = document.createElement('div');
    var span = document.createElement('span');
    span.appendChild(document.createTextNode('Filter:'));
    span.className = 'inputLabel';
	
	//Create checkbox that checks/unchecks all checkboxes in  
	var cbox = document.createElement('input');
	cbox.id = 'AllLoggerToggle';
	var clabel = document.createElement('label');
	clabel.className = 'inputLabel';
	clabel.appendChild(document.createTextNode('Select/Clear All'));
	clabel.setAttribute('for', 'AllLoggerToggle');
	
    var changeHandler = function(event){
    	var s = event.target;
    	if(!s){return false;}
    	var checkem = s.checked;
    	var inputs = logFt.getElementsByTagName('input');
    	for(var i in inputs){
    		if(inputs[i] && inputs[i].nodeType==1 && inputs[i].getAttribute('type')=='checkbox'){
				inputs[i].checked=checkem;
				var category = inputs[i].className.replace(/yui\-log\-filter/, '');
				if(category && category.substr(0,1)=='-'){
    				category = category.substr(1);
    				if(checkem){
    					lR.showCategory(category);
    				}else{
    					lR.hideCategory(category);
    				}
				}else{
    				if(checkem){
    					lR.showSource(category);
    				}else{
    					lR.hideSource(category);
    				}
				}
    		}
    	}
    	if(checkem){
    		//apply filter
    		keyupHandler({target:sinput});
    	}
    }
    
	cbox.setAttribute('type', 'checkbox');
	cbox.setAttribute('checked', true);
	var cboxObj = new YAHOO.util.Element(cbox);
	cboxObj.subscribe('change', changeHandler);
	var cdiv = document.createElement('div');
	cdiv.appendChild(cbox);
	cdiv.appendChild(clabel);
	//Add elements to the console
	sc.appendChild(document.createElement('hr'));
	sc.appendChild(cdiv);
	
	
    sc.appendChild(span); 
    sc.appendChild(sinput);
    
    logFt.appendChild(sc);
    //Hide it and make it open when the user presses ~
	lR.collapse();
	
	return this;
};

YAHOO.ELSA.ConsoleProvider = function(){};
YAHOO.ELSA.ConsoleProvider.prototype = {
    log: function(msg, lvl) {
    	// use the error console if available (FF+FireBug or Safari)
    	if(typeof(console)=='object' && console && typeof(console.log)=='function'){
    		for(var a =0; a<arguments.length;a++){
    			console.log(arguments[a]);
    		}
    	}else{
    		if(!lvl || typeof(lvl)!='string'){
    			lvl='elsa';
    		}else{
    			var lvl_tmp = '';
    			for(var c=0; c<YAHOO.widget.Logger.categories.length; c++){
    				var category = YAHOO.widget.Logger.categories[c];
    				if(category==lvl){
    					lvl_tmp = category;
    				}
    			}
    			lvl = lvl_tmp ? lvl_tmp : 'elsa';
    		}
    		YAHOO.log(msg, lvl);
    	}
    }
};

YAHOO.ELSA.cancelQuery = function(p_oEvent, p_aArgs){
	var iQid = p_aArgs[0];
	// Send xhr to tell the backend to call off the search
	var request = YAHOO.util.Connect.asyncRequest('GET', 
		'Query/cancel_query?qid=' + iQid,
		{ 
			success:function(oResponse){
				var oPanel = new YAHOO.ELSA.Panel('cancel_query');
				oPanel.panel.setHeader('Cancelling Query');
				oPanel.panel.setBody('Cancelling query with ID ' + iQid + '.  You will be able to issue a new archive query soon.  It may take several minutes to cancel the query.');
				oPanel.panel.show();
				return true;
			}, 
			failure:function(oResponse){
				YAHOO.ELSA.Error('Query cancel failed!'); 
				return false;
			}
		}
	);
}

YAHOO.ELSA.getRunningArchiveQuery = function(){
	// Send xhr to find any current archive queries
	var request = YAHOO.util.Connect.asyncRequest('GET', 
		'Query/get_running_archive_query',
		{ 
			success:function(oResponse){
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object'){
						if (oReturn['error']){
							YAHOO.ELSA.Error(oReturn['error']);
						}
						else {
							logger.log('got running query ' + oReturn.qid);
							var oPanel = new YAHOO.ELSA.Panel('cancel_query');
							oPanel.panel.setHeader('Cancel Query');
							if (oReturn.qid && oReturn.qid != 0){
								oPanel.panel.setBody('');
								var aEl = document.createElement('a');
								aEl.innerHTML = 'Cancel Query ' + oReturn.qid;
								aEl.href = '#';
								oPanel.panel.appendToBody(aEl);
								var oEl = new YAHOO.util.Element(aEl);
								oEl.on('click', YAHOO.ELSA.cancelQuery, [oReturn.qid], this);
							}
							else {
								oPanel.panel.setBody('No currently running archive query to cancel.');
							}
							oPanel.panel.show();
							return true;
						}
					}
					else {
						logger.log(oReturn);
						YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
					}
				}
				else {
					YAHOO.ELSA.Error('No response text');
				}
				
			}, 
			failure:function(oResponse){
				YAHOO.ELSA.Error('Query cancel failed!'); 
				return false;
			}
		}
	);
}

YAHOO.ELSA.Query = function(){
	
	
	this.terms = {};
	this.metas = {
		limit: 100 //default limit
	};
	this.freetext = '';
	
	this.resetTerms = function(){
		logger.log('resetting class fields from ', this);
		this.terms = {};
		this.freetext = '';
		
		return true;
	}
	
	this.resetMetas = function(){
		this.metas = {};
		return true;
	}
	
//	this.reset = function(){
//		logger.log('resetting from ', this);
//		this.terms = {};
//		this.metas = {};
//		this.freetext = '';
//		
//		// Reset the class button
//		var oButton = YAHOO.widget.Button.getButton('class_select_button');
//		oButton.set('label', 'All');
//		
//		// Reset the group by button
//		oButton = YAHOO.widget.Button.getButton('groupby_select_button');
//		oButton.set('label', 'None');
//		
//		return true;
//	}
	
	this.results = new YAHOO.ELSA.Results();
	
	this.booleanMap = {
		'OR': '',
		'AND': '+',
		'NOT': '-'
	};
	this.queryBoolean = '';
	this.setBoolean = function(p_sBoolean){
		if (p_sBoolean && typeof this.booleanMap[ p_sBoolean.toUpperCase() ] != 'undefined'){
			this.queryBoolean = this.booleanMap[ p_sBoolean.toUpperCase() ];
		}
		else {
			YAHOO.ELSA.Error('Invalid boolean given: ' + p_sBoolean);
		}
	}
	
	this.submit = function(){
		// apply the start/stop times
		if (YAHOO.util.Dom.get('start_time').value){
			var sStartTime = getDateFromISO(YAHOO.util.Dom.get('start_time').value)/1000;
			this.addMeta('start', sStartTime);
		}
		if (YAHOO.util.Dom.get('end_time').value){
			var sEndTime = getDateFromISO(YAHOO.util.Dom.get('end_time').value)/1000;
			this.addMeta('end', sEndTime);
		}
		logger.log('submitting query: ', this);
		try {
			var oResults = new YAHOO.ELSA.Results.Tabbed.Live(YAHOO.ELSA.tabView, this);
			logger.log('got query results:', oResults);
			this.resetTerms();
		} catch(e) { YAHOO.ELSA.Error(e); }
	}
	
	this.addTermFromOnClick = function(p_oEvent, p_aArgs){
		logger.log('p_oEvent', p_oEvent);
		logger.log('p_aArgs', p_aArgs);
		var p_sField = p_aArgs[0];
		var p_sValue = p_aArgs[1];
		var p_Op = p_aArgs[2];
		var p_oEl = p_aArgs[3];
		this.addTerm(p_sField, p_sValue, p_Op, p_oEl);
	}
	
	this.addTerm = function(p_sField, p_sValue, p_Op, p_oEl){
		if (!p_sValue){
			var aMatches = p_sField.split(/([\:<>=]+)/);
			p_sField = aMatches[0];
			p_Op = aMatches[1];
			p_sValue = aMatches[2];
		}
		if (!p_Op){
			p_Op = '=';
		}
		
		// Quote if necessary
		if (p_sValue.match(/[^a-zA-Z0-9\.\-\@\_]/) && !p_sValue.match(/^\"[^\"]+\"$/)){
			p_sValue = '"' + p_sValue + '"';
		}
		
		logger.log('adding to current query field:' + this.queryBoolean + p_sField + ', val: ' + p_sValue);
		var formField;
		if (p_oEl){
			formField = p_oEl;
		}
		else {
			formField = YAHOO.util.Dom.get(p_sField);
		}
		if (!formField){
			// Must be the main query bar
			formField = 'q';
		}
		var oEl = new YAHOO.util.Element(formField);
		logger.log('oEl', oEl);
		var oQ = YAHOO.util.Dom.get('q');
		if (p_sField){
			if (this.validateTerm(p_sField, p_sValue)){
				var aField = p_sField.split(/\./);
				var sClass = aField[0];
				var sField = aField[1];
				if (!sField){
					sField = sClass;
					sClass = '';
				}
				
				if (sClass == 'any' || sClass == ''){ //special case for 'any' class as it causes issues on the backend
					var oTimeConversions = {
						'timestamp': 1,
						'minute': 60,
						'hour': 3600,
						'day': 86400
					};
					if (oTimeConversions[sField]){
						var oStartDate = getDateFromISO(p_sValue);
						var iMs = oStartDate.getTime();
						logger.log('adding ' + (oTimeConversions[sField] * 1000) + ' to ' + iMs);
						iMs += (oTimeConversions[sField] * 1000);
						var oEndDate = new Date();
						oEndDate.setTime(iMs);
						YAHOO.util.Dom.get('start_time').value = getISODateTime(oStartDate);
						YAHOO.util.Dom.get('end_time').value = getISODateTime(oEndDate);
					}
					else {
						this.terms[p_sField] = p_sValue;
						oQ.value += ' ' + this.queryBoolean + sField + p_Op + p_sValue;
					}
				}
				else {
					this.terms[p_sField] = p_sValue;
					oQ.value += ' ' + this.queryBoolean + sClass + '.' + sField + p_Op + p_sValue;
				}
				
				oEl.removeClass('invalid');
				return true;
			}
			else {
				YAHOO.ELSA.Error('Invalid value ' + p_sValue + ' for field ' + p_sField);
			}
		}
		else {
			// No validation necessary because we don't have a field
			YAHOO.ELSA.Error('added term without a field');
		}
	}
	
	this.delTerm = function(p_sField){
		logger.log('removing current query field:' + p_sField);
		delete this.terms[p_sField];
		return true;
	}
	
	this.addMeta = function(p_sField, p_sValue){
		logger.log('adding to current query meta:' + p_sField + ', val: ' + p_sValue);
		if (this.validateMeta(p_sField, p_sValue)){
			this.metas[p_sField] = p_sValue;
			return true;
		}
		else {
			YAHOO.ELSA.Error('invalid value ' + p_sValue + ' given for meta ' + p_sField);
			return false;
		}
	}
	
	this.delMeta = function(p_sField){
		logger.log('removing current query meta:' + p_sField);
		delete this.metas[p_sField];
		return true;
	}
	
	this.stringifyTerms = function(){
		sQuery = YAHOO.util.Dom.get('q').value.replace(/\ [\-\.]\ /g, ' ');
		return sQuery; //YAHOO.util.Dom.get('q').value;
	}
	
//	this.stringifyMetas = function(){
//		var sQuery = '';
//		logger.log('stringifying: ', this.metas);
//		for (var field in this.metas){
//			logger.log('field: '  + field + ', value: ' + this.metas[field]);
//			sQuery += field + ':' + '"' + this.metas[field] + '"';
//		}
//		logger.log('returning: ' + sQuery);
//		return sQuery;
//	}
	
	this.toString = function(){
		return YAHOO.lang.JSON.stringify( 
			{ 
				'query_string' : this.stringifyTerms(),
				'query_meta_params' : this.metas
			}
		);
	}
	
	this.validateTerm = function(p_sFQDNField, p_sValue){
		logger.log('validating ' + p_sFQDNField + ':' + p_sValue);
		var oField;
		var oMetas = {
			'class': 1,
			'any.class': 1,
			'program': 1,
			'any.program': 1,
			'timestamp': 1,
			'any.timestamp': 1,
			'minute': 1,
			'any.minute': 1,
			'hour': 1,
			'any.hour': 1,
			'day': 1,
			'any.day': 1,
			'node': 1,
			'any.node': 1
		};
		if (oMetas[p_sFQDNField]){
			return this.validateMeta(p_sFQDNField, oMetas[p_sFQDNField]);
		}
		for (var i = 0; i < YAHOO.ELSA.formParams.fields.length; i++){
			if (YAHOO.ELSA.formParams.fields[i].fqdn_field == p_sFQDNField){
				oField = YAHOO.ELSA.formParams.fields[i];
				break;
			}
			else if (YAHOO.ELSA.formParams.fields[i].value == p_sFQDNField){
				oField = YAHOO.ELSA.formParams.fields[i];
				break;
			}
		}
		logger.log('oField:',oField);
		if (!oField){
			return false;
		}
		var oRegex = this.getInputRegex(oField);
		logger.log('testing ' + p_sValue + ' against ' + oField.input_validation);
		return oRegex.test(p_sValue);
	}
	
	this.validateMeta = function(){
		return true;
	}
	
	this.getInputRegex = function(p_oField){
		if (p_oField['input_validation']){
			switch (p_oField['input_validation']){
				case 'IPv4':
					return new RegExp(/^['"]?\d+\.\d+\.\d+\.\d+['"]?$/);
				default:
					YAHOO.ELSA.Error('Unknown input_validation: ' + p_oField['input_validation']);
			}
		}
		else {	
			switch (p_oField['type']){
				case 'int':
					return new RegExp(/^\d+$/);
				case 'string':
					//return new RegExp(/^.+$/);
				default:
					return new RegExp(/^.+$/);
			}
		}
	}
};

YAHOO.ELSA.addTermFromOnClickNoSubmit = function(p_oEvent, p_aArgs){
	YAHOO.ELSA.addQueryTerm(p_aArgs[0], p_aArgs[1], p_aArgs[2]);
}

YAHOO.ELSA.addQueryTerm = function(p_sClass, p_sField, p_sValue){
	logger.log('adding to current query class' + p_sClass + ', field:' + p_sField + ', val: ' + p_sValue);
	try {
		YAHOO.ELSA.currentQuery.addTerm(p_sClass + '.' + p_sField, p_sValue);
	} catch(e) { YAHOO.ELSA.Error(e); }
};

YAHOO.ELSA.addTermFromChart = function(p_iChartId, p_iIndex){
	logger.log('addTermFromChart p_iChartId', p_iChartId);
	logger.log('addTermFromChart p_iIndex', p_iIndex);
	logger.log('chart data: ', YAHOO.ELSA.Charts[p_iChartId]);
	var sField = YAHOO.ELSA.Charts[p_iChartId].cfg.elements[0].text;
	var oData = YAHOO.ELSA.Charts[p_iChartId].cfg.elements[0].values[p_iIndex];
	YAHOO.ELSA.currentQuery.delMeta('class');
	YAHOO.ELSA.currentQuery.delMeta('groupby');
	YAHOO.ELSA.currentQuery.delMeta('groups_only');
	YAHOO.ELSA.currentQuery.delMeta('limit');
	YAHOO.ELSA.addTermAndSubmit(sField, oData);
}

YAHOO.ELSA.addTermFromOnClick = function(p_oEvent, p_aArgs){
	YAHOO.ELSA.addTermAndSubmit(p_aArgs[0], p_aArgs[1]);
}

YAHOO.ELSA.addTermAndSubmit = function(p_sField, p_oData){
	logger.log('p_oData', p_oData);
	var sData;
	if (typeof p_oData != 'object'){
		sData = p_oData;
	}
	else {
		sData = p_oData['label'];
	}
	logger.log('this', this);
	logger.log('type of ' + typeof this);
	var tmp = YAHOO.ELSA.currentQuery.queryBoolean;
	try {
		YAHOO.ELSA.currentQuery.queryBoolean = '+';
		YAHOO.ELSA.currentQuery.addTerm(p_sField, '"' + sData + '"', '=');
		YAHOO.ELSA.currentQuery.delMeta('class');
		YAHOO.ELSA.currentQuery.delMeta('groupby');
		YAHOO.ELSA.currentQuery.delMeta('groups_only');
		YAHOO.ELSA.currentQuery.delMeta('limit');
		YAHOO.ELSA.currentQuery.submit();
	} catch(e) { YAHOO.ELSA.Error(e); }
	
	YAHOO.ELSA.currentQuery.queryBoolean = tmp;
	logger.log('submitted');
}
	

YAHOO.ELSA.groupData = function(p_iId, p_sClass, p_sField, p_sAggFunc){
	logger.log('p_iId', p_iId);
	logger.log('p_sClass', p_sClass);
	if (!YAHOO.ELSA.currentQuery){
		logger.log('no currentQuery');
		return;
	}
	
	// we might have gotten an array ref as args
	if (typeof p_iId == 'object'){
		var arr = p_sClass;
		p_iId = arr[0];
		p_sClass = arr[1];
		p_sField = arr[2];
		p_sAggFunc = arr[3];
	}
	
	// reset old values
	YAHOO.ELSA.currentQuery.delMeta('class');
	YAHOO.ELSA.currentQuery.delMeta('groupby');
	YAHOO.ELSA.currentQuery.delMeta('groups_only');
	YAHOO.ELSA.currentQuery.delMeta('class');
	YAHOO.ELSA.currentQuery.delMeta('limit');
	
	if (!p_sClass){
		YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
		YAHOO.ELSA.currentQuery.addMeta('groupby', [p_sField]);
	}
	else if (p_sClass == 'any'){
		//any class, always an INT field
		YAHOO.ELSA.currentQuery.addMeta('class', 'any');
		YAHOO.ELSA.currentQuery.addMeta('groupby', [p_sField]);
		YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
	}
	else if (p_sClass != YAHOO.ELSA.Labels.noGroupBy){ //clears
		// Find type to determine if we can do this remotely or if it's a client-side group
		var sFieldType = 'string';
		for (var i in YAHOO.ELSA.formParams.fields){
			if (YAHOO.ELSA.formParams.fields[i].fqdn_field === p_sClass + '.' + p_sField){
				sFieldType = YAHOO.ELSA.formParams.fields[i].field_type;
				break;
			}
		}
		
		YAHOO.ELSA.currentQuery.addMeta('class', p_sClass);
		YAHOO.ELSA.currentQuery.addMeta('groupby', [p_sField]);
		YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
	}
	
	// create new groupby results
	var oResults = new YAHOO.ELSA.Results.Tabbed.Live(YAHOO.ELSA.tabView, YAHOO.ELSA.currentQuery);
	logger.log('got query results:', oResults);
	YAHOO.ELSA.currentQuery.resetTerms();
}

YAHOO.ELSA.sendLocalChartData = function(p_iId, p_sField, p_sAggFunc){
	if (!YAHOO.ELSA.localResults[p_iId]){
		YAHOO.ELSA.Error('No results for id ' + p_iId);
		return;
	}
	
	var aData = [];
	for (var i in YAHOO.ELSA.localResults[p_iId].results.results){
		var rec = {};
		for (var j in YAHOO.ELSA.localResults[p_iId].results.results[i]._fields){
			logger.log('matching ' + YAHOO.ELSA.localResults[p_iId].results.results[i]._fields[j].field + ' against ' + p_sField);
			if (YAHOO.ELSA.localResults[p_iId].results.results[i]._fields[j].field == p_sField){
				rec[p_sField] = YAHOO.ELSA.localResults[p_iId].results.results[i]._fields[j].value;
			}
		}
		if (keys(rec).length){
			aData.push(rec);
		}
	}
	logger.log('results:', aData);
	
	if (!p_sAggFunc){
		var sSampleVal = aData[0][p_sField];
		if (sSampleVal.match(/^\d+$/)){
			p_sAggFunc = 'SUM';
		}
		else {
			p_sAggFunc = 'COUNT';
		}
	}
	var sendData = {
		data: aData,
		func: p_sAggFunc 
	};

	var callback = {
		success: function(p_oResponse){
			oSelf = p_oResponse.argument[0];
			if(p_oResponse.responseText !== undefined && p_oResponse.responseText){
				logger.log('rawResponse: ' + p_oResponse.responseText);
				try{
					var oRawChartData = YAHOO.lang.JSON.parse(p_oResponse.responseText);
					logger.log('oRawChartData', oRawChartData);
					var divId = 'chart';
					var oChart = new YAHOO.ELSA.Chart.Auto(divId, 'line', p_sField, oRawChartData);
				}catch(e){
					logger.log('Could not parse response for chart parameters because of an error: '+e);
					return false;
				}				
			}
			else {
				YAHOO.ELSA.Error('Did not receive chart params');
				return false;
			}
		},
		failure: function(oResponse){
			YAHOO.ELSA.Error('Error creating chart.');
		},
		argument: [this]
	};
	
	logger.log('sending: ', 'data=' + YAHOO.lang.JSON.stringify(sendData));
	var oConn = YAHOO.util.Connect.asyncRequest('POST', 'Chart/json', callback, 
		'data=' + encodeURIComponent(YAHOO.lang.JSON.stringify(sendData)));
}

YAHOO.ELSA.Results = function(){
	
	logger.log('before push: ', YAHOO.ELSA.localResults);
	YAHOO.ELSA.localResults.push(this);
	YAHOO.ELSA.queryResultCounter++;
	this.id = YAHOO.ELSA.queryResultCounter;
	logger.log('my id: ' + this.id);
	
	var oSelf = this;
	
	this.formatFields = function(p_elCell, oRecord, oColumn, p_oData){
		//logger.log('called formatFields on ', oRecord);
		try {
			var msgDiv = document.createElement('div');
			msgDiv.setAttribute('class', 'msg');
			if (oSelf.results.highlights){
				//apply highlights
				var msg = cloneVar(oRecord.getData().msg);
				for (var sHighlight in oSelf.results.highlights){
					var re = new RegExp('(' + sHighlight + ')', 'ig');
					var aMatches = msg.match(re);
					if (aMatches != null){
						var sReplacement = '<span class=\'highlight\'>' + escapeHTML(aMatches[0]) + '</span>';
						msg = msg.replace(re, sReplacement);
					}
				}
			}
			msgDiv.innerHTML = msg;
			p_elCell.appendChild(msgDiv);
			
			var oDiv = document.createElement('div');
			var oTempWorkingSet = cloneVar(p_oData);
			
			for (var i in oTempWorkingSet){
				var fieldHash = oTempWorkingSet[i];
				fieldHash.value_with_markup = escapeHTML(fieldHash.value);
				//logger.log('fieldHash', fieldHash);
				
				// create chart link
				var oGraphA = document.createElement('a');
				oGraphA.innerHTML = fieldHash['field'];
				oGraphA.setAttribute('href', '#');
				oGraphA.setAttribute('class', 'key');
				oDiv.appendChild(oGraphA);
				var oElGraphA = new YAHOO.util.Element(oGraphA);
				oElGraphA.on('click', YAHOO.ELSA.groupData, [ YAHOO.ELSA.getLocalResultId(oSelf.tab), fieldHash['class'], fieldHash['field'] ], this);
				
				// create drill-down item link
				var a = document.createElement('a');
				a.id = oRecord.getData().id + '_' + fieldHash['field'];
				
				a.setAttribute('href', '#');//Will jump to the top of page. Could be annoying
				a.setAttribute('class', 'value');
				
				if (oSelf.results.highlights){
					for (var sHighlight in oSelf.results.highlights){
						var re = new RegExp('(' + sHighlight + ')', 'ig');
						//logger.log('str: ' + fieldHash['value_with_markup'] + ', re:' + re.toString());
						if (fieldHash['value_with_markup']){
							var re = new RegExp('(' + RegExp.escape(sHighlight) + ')', 'ig');
							var aMatches = msg.match(re);
							if (aMatches != null){
								var sReplacement = '<span class=\'highlight\'>' + escapeHTML(aMatches[0]) + '</span>';
								fieldHash['value_with_markup'] = fieldHash['value_with_markup'].replace(re, sReplacement);
							}
						}
						else {
							fieldHash['value_with_markup'] = '';
						}
						a.innerHTML = fieldHash['value_with_markup'];
					}
				}
				
				oDiv.appendChild(document.createTextNode('='));
				oDiv.appendChild(a);
				
				var oAEl = new YAHOO.util.Element(a);
				oAEl.on('click', YAHOO.ELSA.addTermFromOnClickNoSubmit, [fieldHash['class'], fieldHash['field'], fieldHash['value']]);
				oDiv.appendChild(document.createTextNode(' '));
			}
			p_elCell.appendChild(oDiv);
		}
		catch (e){
			logger.log('exception while parsing field:', e);
			return '';
		}
	}
	
//	this.formatAddHighlights = function(p_elCell, oRecord, oColumn, p_oData){
//		var sText = p_oData;
//		for (var sHighlight in oSelf.highlights){
//			var re = new RegExp('(' + RegExp.escape(sHighlight) + ')', 'ig');
//			sText = sText.replace(re, '<span class="highlight">$1</span>');
//		}
//		p_elCell.innerHTML = sText;
//	}
	
	this.formatDate = function(p_elCell, oRecord, oColumn, p_oData)
	{
		var oDate = p_oData;
		if(p_oData instanceof Date){
			oDate = p_oData;
		}else{
			var mSec = Date.parse(p_oData);
			oDate = new Date();
			oDate.setTime(mSec);
		}
		var curDate = new Date();
		// only display the year if it isn't the current year
		if (curDate.getYear() != oDate.getYear()){
			p_elCell.innerHTML = YAHOO.ELSA.TimeTranslation.Days[ oDate.getDay() ] + ' ' +  
			YAHOO.ELSA.TimeTranslation.Months[ oDate.getMonth() ] + ' ' + oDate.getDate() +
			oDate.getYear() + ' ' + oDate.getHours() + ':' + oDate.getMinutes() + ':' + oDate.getSeconds();
		}
		else {
			p_elCell.innerHTML = sprintf('%s %s %02d %02d:%02d:%02d',
				YAHOO.ELSA.TimeTranslation.Days[ oDate.getDay() ],
				YAHOO.ELSA.TimeTranslation.Months[ oDate.getMonth() ],
				oDate.getDate(),
				oDate.getHours(),
				oDate.getMinutes(),
				oDate.getSeconds()
			);
		}
	};
	
	this.formatInfoButton = function(p_elCell, p_oRecord, p_oColumn, p_oData){
		//logger.log('oRecord.getData()', oRecord.getData());
		try {
			var oA = document.createElement('a');
			oA.href = '#';
			oA.id = 'button_' + oSelf.id + '_' + p_oRecord.getId();
			oA.name = 'button_' + p_oRecord.getId();
			oA.innerHTML = 'Info';
			p_elCell.appendChild(oA);
			var oAEl = new YAHOO.util.Element(oA);
			oAEl.addClass('infoButton');
			oAEl.subscribe('click', YAHOO.ELSA.getInfo, p_oRecord);
		}
		catch (e){
			var str = '';
			for (var i in e){
				str += i + ' ' + e[i];
			}
			YAHOO.ELSA.Error('Error creating button: ' + str);
		}
	}
	
	this.send =  function(p_sPlugin, p_sUrl){ 
		YAHOO.ELSA.send(p_sPlugin, p_sUrl, this.results.results);
	}
	
	this.save = function(p_sComment){
		//logger.log('saveResults', this);
		if (!this.results){
			throw new Error('No results to save');
		}
		
		var callback = {
			success: function(oResponse){
				oSelf = oResponse.argument[0];
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object'){
						YAHOO.ELSA.Error(oReturn['error']);
					}
					else if (oReturn == 1) {
						logger.log('result saved successfully');
						var lbl = '';
						lbl = oSelf.tab.get('label');
						lbl = '(Saved to QID ' + oSelf.id + ') ' + lbl;
						oSelf.tab.set('label', lbl);
					}
					else {
						logger.log(oReturn);
						YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
					}
				}
				else {
					YAHOO.ELSA.Error('No response text');
				}
			},
			failure: function(oResponse){
				YAHOO.ELSA.Error('Error saving result.');
			},
			argument: [this]
		};
		
		var closeDialog = function(){
			var eD = YAHOO.util.Dom.get('exportDialog');
			eD.parentNode.removeChild(eD);
		}

		logger.log('sending: ', 'comments=' + p_sComment + '&results=' + YAHOO.lang.JSON.stringify(this.results));
		var oConn = YAHOO.util.Connect.asyncRequest('POST', 'Query/save_results', callback, 
			'comments=' + p_sComment + '&results=' + encodeURIComponent(YAHOO.lang.JSON.stringify(this.results)));
	};
	
	this.createDataTable = function(p_oResults, p_oElContainer){
		var oFields = [
			{ key:"id", parser:parseInt },
			{ key:"timestamp", parser:YAHOO.util.DataSourceBase.parseDate },
			{ key:"host", parser:YAHOO.util.DataSourceBase.parseString },
			{ key:"class", parser:YAHOO.util.DataSourceBase.parseString },
			{ key:"program", parser:YAHOO.util.DataSourceBase.parseString },
			{ key:"_fields"/*, parser:this.fieldParser*/ },
			{ key:"msg", parser: escapeHTML }
		];
		
		var oColumns = [
			{ key:'info', label:"", sortable:true, formatter:this.formatInfoButton },
			{ key:"timestamp", label:"Timestamp", sortable:true, editor:"date", formatter:this.formatDate },
			{ key:"_fields", label:"Fields", sortable:true, formatter:this.formatFields } //parser adds highlights
		];
		
		// DataSource instance
	    this.dataSource = new YAHOO.util.DataSource(p_oResults);
	    this.dataSource.maxCacheEntries = 4; //cache these
	    this.dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
	    this.dataSource.responseSchema = {
	        resultsList: "results",
	        fields: oFields,
	        metaFields: {
	            totalRecords: "totalRecords", // Access to value in the server response
	            recordsReturned: "recordsReturned",
	            startIndex: "startIndex"
	        }
	    };
	    
	    this.paginator = new YAHOO.widget.Paginator({
	        pageLinks          : 10,
	        rowsPerPage        : 15,
	        rowsPerPageOptions : [15,50,100],
	        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
	        pageReportTemplate : "<strong>Records: {totalRecords} / " + this.dataSource.liveData.totalRecords + " </strong> "
	        	+ this.dataSource.liveData.totalTime + ' ms '
	    });
	    
	    var oTableCfg = {
	        paginator: this.paginator,
	        dynamicData: false,
	        summary: 'this is a summary'
	    };
	    
	    try{
	    	logger.log('About to create DT with ', "dt"+this.id, oColumns, this.dataSource, oTableCfg);
	    	this.dataTable = new YAHOO.widget.DataTable(p_oElContainer, oColumns, this.dataSource, oTableCfg);
	    	logger.log('datatable: ', this.dataTable);
	  	 	YAHOO.util.Dom.removeClass(p_oElContainer, 'hiddenElement');
	    }catch(e){
	    	logger.log('No datatable because:', e);
	    	for (var term in e){
				logger.log(term, e[term]);
			}
	    	return;
	    }
	}
	
	this.createGroupByDataTable = function(p_oGroupBy, p_sGroupBy, p_oElContainer){
		if (!this.groupByDataTables){
			this.groupByDataTables = {};
		}
		logger.log('p_oGroupBy', p_oGroupBy);
		logger.log('p_sGroupBy', p_sGroupBy);
		var oGroupData = p_oGroupBy;
		logger.log('oGroupData', oGroupData);
		
		// create data formatted for chart
		var aX = [];
		var aY = [];
		for (var i in oGroupData){
			var oRec = oGroupData[i];
			aX.push(oRec['@groupby']);
			aY.push(oRec['@count']);
		}
		var oChartData = {
			x: aX
		};
		oChartData[p_sGroupBy] = aY;
		logger.log('oChartData:', oChartData);
		this.chartData = oChartData;
		
		// create data table data
		var aExportData = [];
		for (var i = 0; i < oGroupData.length; i++){
			oGroupData[i]['count'] = oGroupData[i]['@count'];
			oGroupData[i]['groupby'] = oGroupData[i]['@groupby'];
			aExportData.push({count:oGroupData[i]['@count'], groupby:oGroupData[i]['@groupby']});
		}
		
		// Create a container for the button
		var buttonEl = document.createElement('div');
		buttonEl.id = 'groupby_button_' + this.id + '_' + p_sGroupBy;
		p_oElContainer.appendChild(buttonEl);
		
		// Create the export button
		var oMenuSources = [ 
			{text:'Save Results', value:'saveResults', onclick: { fn: YAHOO.ELSA.saveResults, obj:this.id }},
			{text:'Export Results', value:'exportResults', onclick: { fn: YAHOO.ELSA.exportData, obj:aExportData }}
		];
		
		var oMenuButtonCfg = {
			type: 'menu',
			label: 'Result Options...',
			name: 'result_options_select_button',
			menu: oMenuSources,
			container: buttonEl
		};
		var oButton = new YAHOO.widget.Button(oMenuButtonCfg);
		
		// create div to hold both datatable and grid
		var bothDiv = document.createElement('div');
		p_oElContainer.appendChild(bothDiv);
		
		// Create a container for the datatable
		var dtEl = document.createElement('div');
		dtEl.id = 'groupby_datatable_' + this.id + '_' + p_sGroupBy;
		var oEl = new YAHOO.util.Element(dtEl);
		oEl.setStyle('float', 'left');
		bothDiv.appendChild(dtEl);
		
		// create a div for the chart and create it with the local data
		var oChartEl = document.createElement('div');
		oChartEl.id = 'groupby_chart_' + this.id + '_' + p_sGroupBy;
		oEl = new YAHOO.util.Element(oChartEl);
		oEl.setStyle('float', 'left');
		bothDiv.appendChild(oChartEl);
		logger.log('p_oElContainer: ' + p_oElContainer.innerHTML);
		var sTitle = this.tab.get('labelEl').innerText;
		var oChart = new YAHOO.ELSA.Chart.Auto(oChartEl.id, 'bar', sTitle, this.chartData, YAHOO.ELSA.addTermFromChart);
		
		var formatValue = function(p_elCell, oRecord, oColumn, p_oData){
			var a = document.createElement('a');
			a.setAttribute('href', '#');
			a.innerHTML = p_oData;
			var el = new YAHOO.util.Element(a);
			el.on('click', YAHOO.ELSA.addTermFromOnClick, [p_sGroupBy, p_oData], YAHOO.ELSA.currentQuery);
			p_elCell.appendChild(a);
		}
		
		var oFields = [
			{ key:'count', parser:YAHOO.util.DataSourceBase.parseNumber },
			{ key:'groupby', parser:YAHOO.util.DataSourceBase.parseString }
		];
		
		var oColumns = [
			{ key:'count', label:'Count', sortable:true },
			{ key:'groupby', label:'Value', formatter:formatValue, sortable:true }
		];
		
		// DataSource instance
	    var dataSource = new YAHOO.util.DataSource(p_oGroupBy);
	    dataSource.maxCacheEntries = 4; //cache these
		dataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
	    dataSource.responseSchema = {
	        //resultsList: p_sGroupBy,
	        fields: oFields
	    };
	    
	    var oTableCfg = { };
	    try{
	    	logger.log('About to create DT with ', dtEl, oColumns, dataSource, oTableCfg);
	    	this.groupByDataTables[p_sGroupBy] = new YAHOO.widget.DataTable(dtEl, oColumns, dataSource, oTableCfg);
	  	 	logger.log('groupby datatable: ', this.groupByDataTables[p_sGroupBy]);
	  	 	YAHOO.util.Dom.removeClass(dtEl, 'hiddenElement');
	    }catch(e){
	    	logger.log('No datatable because:', e);
	    	for (var term in e){
				logger.log(term, e[term]);
			}
	    	return;
	    }
	}
};

YAHOO.ELSA.Results.Given = function(p_oResults){
	this.superClass = YAHOO.ELSA.Results;
	this.superClass();
	this.results = p_oResults;
	
	this.formatFields = function(p_elCell, oRecord, oColumn, p_oData){
		//logger.log('called formatFields on ', oRecord);
		try {
			var msgDiv = document.createElement('div');
			msgDiv.setAttribute('class', 'msg');
			var msg = cloneVar(oRecord.getData().msg);
			msgDiv.innerHTML = msg;
			p_elCell.appendChild(msgDiv);
			
			var oDiv = document.createElement('div');
			var oTempWorkingSet = cloneVar(p_oData);
			
			for (var i in oTempWorkingSet){
				var fieldHash = oTempWorkingSet[i];
				fieldHash.value_with_markup = escapeHTML(fieldHash.value);
				//logger.log('fieldHash', fieldHash);
				
				// create field text
				var oText = document.createTextNode(fieldHash['field'] + '=' + fieldHash['value'] + ' ');
				oDiv.appendChild(oText);
			}
			p_elCell.appendChild(oDiv);
		}
		catch (e){
			logger.log('exception while parsing field:', e);
			return '';
		}
	}
	
	var oDiv = document.createElement('div');
	oDiv.id = 'given_results';
	YAHOO.util.Dom.get('logs').appendChild(oDiv);
	
	this.createDataTable(p_oResults, oDiv);
	
	this.dataTable.render();
}
	
YAHOO.ELSA.Form = function(p_oFormEl, p_oFormCfg){
	this.form = p_oFormEl;
	this.grid = p_oFormCfg['grid'];
	
	var oTable = document.createElement('table');
	var oTbody = document.createElement('tbody'); //tbody is critical for proper IE appendChild
	
	/* First, find the max width of the grid */
	var iMaxWidth = 0;
	for (var i = 0; i < this.grid.length; i++){
		if (this.grid[i].length > iMaxWidth){
			iMaxWidth = this.grid[i].length;
		}
	}
	
	for (var attr in p_oFormCfg['form_attrs']){
		this.form.setAttribute(attr, p_oFormCfg['form_attrs'][attr]);
	}
	for (var i = 0; i < this.grid.length; i++){
		var iColspan = 0;
		if (this.grid[i].length < iMaxWidth){
			iColspan = iMaxWidth - this.grid[i].length + 1;
		}
		var oTrEl = document.createElement('tr');
		for (var j = 0; j < this.grid[i].length; j++){
			var oTdEl = document.createElement('td');
			/* Adjust iColspan if necessary */
			if (iColspan > 0 && j == (this.grid[i].length - 1)){
				oTdEl.setAttribute('colspan', iColspan);
			}
			
			//Check to see if this is yet another array, and if it is, we'll concat all the objects to make the td
			if (this.grid[i][j].type){
				this.appendItem(oTdEl, this.grid[i][j]);
			}
			else {
				//Must be an array
				for (var k = 0; k < this.grid[i][j].length; k++){
					this.appendItem(oTdEl, this.grid[i][j][k]);
				}
			}
			oTrEl.appendChild(oTdEl);
		}
		oTbody.appendChild(oTrEl);
	}
	oTable.appendChild(oTbody);
	this.form.appendChild(oTable);
	
	this.validate = function(){
		for (var i = 0; i < this.grid.length; i++){
			for (var j = 0; j < this.grid[i].length; j++){
				if (this.grid[i][j].regex){
					var oFormInput = this.grid[i][j];
					var id = oFormInput.args.id;
					logger.log('regex: ' + oFormInput.regex);
					var oInputEl = YAHOO.util.Dom.get(id);
					logger.log('oInputEl:', oInputEl);
					logger.log('value:', oInputEl.value);
					if (oInputEl && oInputEl.value && !oFormInput.regex.test(oInputEl.value)){
						var oEl = new YAHOO.util.Element(id);
						oEl.addClass('invalid');
						return false;
					}
				}
			}
		}
		return true;
	}
	
	this.getValues = function(){
		var oValues = {};
		for (var i = 0; i < this.grid.length; i++){
			for (var j = 0; j < this.grid[i].length; j++){
				if (this.grid[i][j].args && this.grid[i][j].args.id){
					var id = this.grid[i][j].args.id;
					var oInputEl = YAHOO.util.Dom.get(id);
					if (oInputEl.value){
						oValues[id] = oInputEl.value;
					}
				}
			}
		}
		return oValues;
	}
	
	return this;
};

YAHOO.ELSA.Form.prototype.appendItem = function(p_oEl, p_oArgs){
	if (p_oArgs.type == 'text'){
		var oTextNode = document.createTextNode(p_oArgs.args);
		p_oEl.appendChild(oTextNode);;
	}
	else if (p_oArgs.type == 'widget') {
		if (typeof p_oArgs.args.container == 'undefined'){
			/* Set the container for the Button to be this td element */
			p_oArgs.args.container = p_oEl;
		}
		/* Dynamically create the widget object with an eval */
		var sClassName = 'YAHOO.widget.' + p_oArgs.className;
		var form_part;
		eval ('form_part = new ' + sClassName + ' (p_oArgs.args);');
		logger.log('form_part', form_part);
		if (p_oArgs.callback){
			p_oArgs.callback(p_oArgs, form_part, p_oEl);
		}
		// register with overlay manager if a menu
		if (sClassName === 'Menu'){
			YAHOO.ELSA.overlayManager.register(form_part);
		}
		//YUI does the appendChild() for us in the widget construction so we don't have to...
	}
	else if (p_oArgs.type == 'input'){
		var oInputEl = document.createElement('input');
		for (var arg in p_oArgs.args){
			oInputEl[arg] = p_oArgs.args[arg];
		}
		p_oEl.appendChild(oInputEl);
		if (p_oArgs.args.label){
			var elText = document.createElement('label');
			elText.innerHTML = p_oArgs.args.label;
			elText['for'] = p_oArgs.args.id;
			p_oEl.appendChild(elText);
		}
		if (p_oArgs.callback){
			p_oArgs.callback(p_oArgs, oInputEl, p_oEl);
		}
	}
	else if (p_oArgs.type == 'element'){
		logger.log('element args:', p_oArgs);
		var oEl = document.createElement(p_oArgs.element);
		for (var arg in p_oArgs.args){
			oEl[arg] = p_oArgs.args[arg];
		}
		p_oEl.appendChild(oEl);
	}
	else {
		throw 'Unknown grid type: ' + p_oArgs.type;
	}
};

YAHOO.ELSA.Query.Scheduled = function(p_oRecord){
	logger.log('building scheduled query with oRecord:', p_oRecord);
	var data = p_oRecord.getData();
	this.superClass = YAHOO.ELSA.Query;
	this.superClass();
	this.scheduleId = parseInt(data.id);
	this.query = data.query;
	this.interval = data.interval;
	this.start = data.start;
	this.end = data.end;
	this.action = data.action;
	this.action_params = data.action_params;
	this.enabled = data.enabled;
	this.recordSetId = YAHOO.ELSA.getQuerySchedule.dataTable.getRecordSet().getRecordIndex(p_oRecord);
	
	this.set = function(p_sProperty, p_oNewValue){
		this[p_sProperty] = p_oNewValue; // set
		
		// sync to server
		var callback = {
			success: function(oResponse){
				oSelf = oResponse.argument[0];
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object'){
						if (oReturn['error']){
							YAHOO.ELSA.Error(oReturn['error']);
							YAHOO.ELSA.getQuerySchedule.asyncSubmitterCallback();
						}
						else {
							logger.log('updated successfully, return:',oReturn);
							for (var arg in oReturn){
								YAHOO.ELSA.getQuerySchedule.asyncSubmitterCallback(true, oReturn[arg]);
							}
						}
					}
					else {
						logger.log(oReturn);
						YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
						YAHOO.ELSA.getQuerySchedule.asyncSubmitterCallback();
					}
				}
				else {
					YAHOO.ELSA.Error('No response text');
					YAHOO.ELSA.getQuerySchedule.asyncSubmitterCallback();
				}
			},
			failure: function(oResponse){
				YAHOO.ELSA.Error('Error saving result.');
				return [ false, ''];
			},
			argument: [this]
		};
		var str = this[p_sProperty];
		if (typeof str == 'object'){
			str = YAHOO.lang.JSON.stringify(str);
		}
		var oConn = YAHOO.util.Connect.asyncRequest('POST', 'Query/update_scheduled_query', callback,
			'id=' + this.scheduleId + '&' +  p_sProperty + '=' + str);
	};
	
	this.remove = function(){
		var removeCallback = {
			success: function(oResponse){
				oSelf = oResponse.argument[0];
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object'){
						if (oReturn['error']){
							YAHOO.ELSA.Error(oReturn['error']);
						}
						else {
							logger.log('deleted query ' + oSelf.scheduleId);
							// find the row in the data table and delete it
							YAHOO.ELSA.getQuerySchedule.dropRow(oSelf.recordSetId);
							YAHOO.ELSA.localResults[oSelf.id] = null;
						}
					}
					else {
						logger.log(oReturn);
						YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
					}
				}
				else {
					YAHOO.ELSA.Error('No response text');
				}
			},
			failure: function(oResponse){ YAHOO.ELSA.Error('Error deleting scheduled query ' + this.scheduleId); },
			argument: [this]
		};
		var oConn = YAHOO.util.Connect.asyncRequest('POST', 'Query/delete_scheduled_query', removeCallback,
			'id=' + this.scheduleId);
	}
};

YAHOO.ELSA.Results.Saved = function(p_iQid){
	logger.log('building saved results with p_iQid:', p_iQid);
	this.superClass = YAHOO.ELSA.Results;
	this.qid = p_iQid;
	
	this.receiveResponse = function(oResponse){
		logger.log('response: ');
		if (oResponse.responseText){
			var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
			logger.log('oReturn:', oReturn);
			if (typeof oReturn === 'object' && oReturn['error']){
				YAHOO.ELSA.Error(oReturn['error']);
			}
			else if (oReturn){
				//oQuery is this from the current scope
				var oSelf = oResponse.argument[0];
				for (var key in oReturn){
					try {
						oSelf[key] = YAHOO.lang.JSON.parse(oReturn[key]);
					}
					catch (e){
						logger.log('key ' + key + ' threw ' + e);
						oSelf[key] = oReturn[key];
					}
				}
			}
			else {
				logger.log(oReturn);
				YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
			}
		}
		else {
			YAHOO.ELSA.Error('No response text');
		}
	};
	
	this.remove = function(){
		var removeCallback = {
			success: function(oResponse){
				oSelf = oResponse.argument[0];
				if (oResponse.responseText){
					var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
					if (typeof oReturn === 'object'){
						if (oReturn['error']){
							YAHOO.ELSA.Error(oReturn['error']);
						}
						else {
							logger.log('deleted query ' + oSelf.qid);
							// find the row in the data table and delete it
							for (var i = 0; i < YAHOO.ELSA.getSavedQueries.dataTable.getRecordSet().getLength(); i++){
								var oRecord = YAHOO.ELSA.getSavedQueries.dataTable.getRecordSet().getRecord(i);
								if (!oRecord){
									continue;
								}
								if (oRecord.getData().qid == oSelf.qid){
									logger.log('removing record ' + oRecord.getId() + ' from datatable');
									YAHOO.ELSA.getSavedQueries.dataTable.deleteRow(oRecord.getId());
									break;
								}
							}
						}
					}
					else {
						logger.log(oReturn);
						YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
					}
				}
				else {
					YAHOO.ELSA.Error('No response text');
				}
			},
			failure: function(oResponse){ YAHOO.ELSA.Error('Error deleting saved query ' + this.qid); },
			argument: [this]
		};
		var oConn = YAHOO.util.Connect.asyncRequest('POST', 'Query/delete_saved_results', removeCallback,
			'qid=' + this.qid);
	};
	
	// Get the saved query data from the server
	var request = YAHOO.util.Connect.asyncRequest('GET', 
			'Query/get_saved_queries?qid=' + p_iQid,
			{ 
				success: this.receiveResponse,
				failure:function(oResponse){
					YAHOO.ELSA.Error('Query failed!'); return false;
				},
				argument: [this]
			}
	);
	
	
};

YAHOO.ELSA.Results.Tabbed = function(p_oTabView, p_sQueryString, p_sTabLabel){
	this.superClass = YAHOO.ELSA.Results;
	this.superClass();
	
	this.queryString = p_sQueryString; 
	
//	// Create the search highlight terms
//	var aQueryWords = p_sQueryString.split(/\s+/);
//	for (var i in aQueryWords){
//		if (!aQueryWords[i]){
//			continue;
//		}
//		
//		aQueryWords[i] = aQueryWords[i].replace(/[^a-zA-Z0-9\.\-\@\_\:\=\>\<]/g, ''); //strip non-alpha-num
//		var aHighlightTerms = aQueryWords[i].split(/[=<>:]+/);
//		logger.log('aHighlightTerms', aHighlightTerms);
//		if (aHighlightTerms.length == 1){
//			if (aHighlightTerms[0]){
//				this.highlights[ aHighlightTerms[0] ] = 1;
//			}
//		}
//		else if (aHighlightTerms.length == 2){
//			if (aHighlightTerms[1]){
//				this.highlights[ aHighlightTerms[1] ] = 1;
//			}
//		}
//	}
	
	this.tabView = p_oTabView;
	YAHOO.util.Dom.removeClass(this.tabView, 'hiddenElement');
	this.tab = new YAHOO.widget.Tab();
	
	var oLabelEl = new YAHOO.util.Element(this.tab.get('labelEl'));
	oLabelEl.addClass('yui-panel');
	
	logger.log('tab: ', this.tab);
	try {
		this.tabView.addTab(this.tab);
		this.tabId = this.tabView.getTabIndex(this.tab);
		this.tab.get('labelEl').innerHTML = 
			'<table id="' + this.id + '" style="padding: 0px;"><tr><td class="yui-skin-sam">' + p_sTabLabel + '</td>' +
			'<td id="close_box_' + this.id + '" class="yui-skin-sam loading"></td></tr></table>';
		var oElClose = new YAHOO.util.Element(YAHOO.util.Dom.get('close_box_' + this.id));
		oElClose.removeClass('hiddenElement');
		
		this.closeTab = function(p_oEvent){
			logger.log('closing tab: ', this);
			YAHOO.util.Event.stopEvent(p_oEvent);
			// find the localResults associated and remove them
			logger.log('removing tab with tabid: ' + this.tabId);
			var iLocalResultId = YAHOO.ELSA.getLocalResultId(this.tab);
			YAHOO.ELSA.localResults.splice(iLocalResultId, 1);
			this.tabView.deselectTab(this.tabId);
			this.tabView.removeTab(this.tab);
			YAHOO.ELSA.updateTabIds(this.tabId);
			this.tabId = '';
			this.tab = '';
		}
		
		// Create a div we'll attach the results menu button to later
		var oEl = document.createElement('div');
		oEl.id = 'query_export_' + this.id;
		this.tab.get('contentEl').appendChild(oEl);
		oElClose.addListener('click', this.closeTab, this, true);
	} catch (e){ logger.log(e) }
	
	this.loadResponse = function(p_oResults){
		logger.log('got results:', p_oResults);
		try {
			this.results = p_oResults;
			var oElClose = new YAHOO.util.Element(YAHOO.util.Dom.get('close_box_' + this.id));
			oElClose.removeClass('loading');
			oElClose.addClass('close');
			var oLabelEl = this.tab.get('labelEl').getElementsByTagName('td')[0];
			
			if (this.results.batch_query){
				oLabelEl.innerHTML += ' [batched]';
			}
			else {
				this.qid = this.results.qid;
				oLabelEl.innerHTML += ' (' + this.results.totalRecords + ')';
		    	if (p_oResults.query_string){ //saved result
			    	this.sentQuery = YAHOO.lang.JSON.stringify({
						query_string: p_oResults.query_string, 
						query_meta_params: p_oResults.query_meta_params
					});
		    	}
			}
		}
		catch (e){
			YAHOO.ELSA.Error('Error loading response' + e);
		}
		
		if (this.results.batch_query){
			var oEl = document.createElement('h3');
			oEl.innerHTML = 'Query ' + this.results.batch_query + ' submitted.  You will receive an email with your results.<br>';
			this.tab.get('contentEl').appendChild(oEl);
			var aEl = document.createElement('a');
			aEl.innerHTML = 'Cancel Query';
			aEl.href = '#';
			this.tab.get('contentEl').appendChild(aEl);
			var oEl = new YAHOO.util.Element(aEl);
			oEl.on('click', YAHOO.ELSA.cancelQuery, [this.results.batch_query], this);
		}
		else if (this.results.groupby && this.results.groupby.length){
			oLabelEl.innerHTML += ' [Grouped by ' + this.results.groupby.join(',') + ']';
			try {
				for (var i in this.results.groupby){
					var sGroupBy = this.results.groupby[i];
					this.createGroupByDataTable(this.results.results[sGroupBy], sGroupBy, this.tab.get('contentEl'));
					this.groupByDataTables[sGroupBy].render();				
					this.groupByDataTables[sGroupBy].sortColumn(
						this.groupByDataTables[sGroupBy].getColumn('count'), 
						YAHOO.widget.DataTable.CLASS_DESC);
				}
			}
			catch (e){
				logger.log('Datatable render failed because:', e.stack);
			}
		}
		else if (typeof(this.results.query_meta_params) != 'undefined' && 
			typeof(this.results.query_meta_params.groupby) != 'undefined' && 
			this.results.query_meta_params.groupby.length){
			oLabelEl.innerHTML += ' [Grouped by ' + this.results.query_meta_params.groupby.join(',') + ']';
			try {
				for (var i in this.results.query_meta_params.groupby){
					var sGroupBy = this.results.query_meta_params.groupby[i];
					this.createGroupByDataTable(this.results.results[sGroupBy], sGroupBy, this.tab.get('contentEl'));
					this.groupByDataTables[sGroupBy].render();				
					this.groupByDataTables[sGroupBy].sortColumn(
						this.groupByDataTables[sGroupBy].getColumn('count'), 
						YAHOO.widget.DataTable.CLASS_DESC);
				}
			}
			catch (e){
				logger.log('Datatable render failed because:', e.stack);
			}
		}
		else {
			var oEl = document.createElement('div');
			oEl.id = 'query_data_table_' + this.id;
	    	this.tab.get('contentEl').appendChild(oEl);
	    	this.createDataTable(this.results, oEl);
			//logger.log('groups: ', this.results.groups);
			//logger.log('length: ' + keys(this.results.groups).length);
			
			try {
				this.renderDataTableHeader();
				this.dataTable.render();
				this.dataTable.sortColumn(
				this.dataTable.getColumn('timestamp'), 
				YAHOO.widget.DataTable.CLASS_ASC);
			}
			catch (e){
				logger.log('Datatable render failed because:', e.stack);
				logger.log('e', e);
			}
		}
		this.tabView.selectTab(this.tabId);
	}
	
	this.renderDataTableHeader = function(){
		var headerContainerDiv = YAHOO.util.Dom.get('query_export_' + this.id);
		var buttonContainerDiv = document.createElement('div');
		buttonContainerDiv.id = 'query_export_' + this.id + '_button';
		var oEl = new YAHOO.util.Element(buttonContainerDiv);
		oEl.setStyle('float', 'left');
		headerContainerDiv.appendChild(buttonContainerDiv);
		
		var aCheckedMenuItems = [];
		if (typeof YAHOO.ELSA.IsAdmin != 'undefined'){
			aCheckedMenuItems = [
				{text:'Get Pcap', value:'getPcap', onclick:{ fn:YAHOO.ELSA.getPcap, obj:this}}
			];
		}
		
		//	Create an array of YAHOO.widget.MenuItem configuration properties
		var oMenuSources = [ 
			{text:'Save Results...', value:'saveResults', onclick: { fn: YAHOO.ELSA.saveResults, obj:this.id }},
			{text:'Export Results...', value:'exportResults', onclick: { fn: YAHOO.ELSA.exportResults, obj:this.id }},
			{text:'Schedule...', value:'schedule', onclick:{ fn:YAHOO.ELSA.scheduleQuery, obj:this.results.qid}},
			{text:'Alert...', value:'alert', onclick:{	fn:YAHOO.ELSA.createAlert, obj:this.results.qid}},
			{text:'Open Ticket...', value:'open_ticket', onclick:{ fn:YAHOO.ELSA.openTicket, obj:this.results.qid}},
			{text:'Checked', submenu: {id: 'checked_menu_' + this.id, itemdata:aCheckedMenuItems }}
		];
		
		var oMenuButtonCfg = {
			type: 'menu',
			label: 'Result Options...',
			name: 'result_options_select_button',
			menu: oMenuSources,
			container: buttonContainerDiv
		};
		var oButton = new YAHOO.widget.Button(oMenuButtonCfg);
		logger.log('rendering to ', this.tab.get('contentEl'));
		
		// If there were any errors, display them
		if (this.results.errors && this.results.errors.length > 0){
			var elErrors = document.createElement('b');
			elErrors.innerHTML = 'Errors: ' + this.results.errors.join(', ');
			headerContainerDiv.appendChild(elErrors);
			var oElErrorsDiv = new YAHOO.util.Element(elErrors);
			oElErrorsDiv.addClass('warning');
			headerContainerDiv.appendChild(document.createElement('br'));
		}
		
		// If there were any warnings, display them
		if (this.results.warnings && this.results.warnings.length > 0){
			var elWarnings = document.createElement('b');
			elWarnings.innerHTML = 'Warnings: ' + this.results.warnings.join(', ');
			headerContainerDiv.appendChild(elWarnings);
			var oElWarningsDiv = new YAHOO.util.Element(elWarnings);
			oElWarningsDiv.addClass('warning');
			headerContainerDiv.appendChild(document.createElement('br'));
		}
		
		// create a summary of fields contained within the data as a quick link for navigation
		var elTextNode = document.createElement('b');
		elTextNode.innerHTML = 'Field Summary';
		headerContainerDiv.appendChild(elTextNode);
		
		var oUniqueFields = {};
		for (var i = 0; i < this.dataTable.getRecordSet().getLength(); i++){
			var oRecord = this.dataTable.getRecordSet().getRecord(i);
			for (var j in oRecord.getData()._fields){
				var oFieldHash = oRecord.getData()._fields[j];
				var sFieldName = oFieldHash.field;
				if (typeof oUniqueFields[sFieldName] != 'undefined'){
					oUniqueFields[sFieldName].count++;
					oUniqueFields[sFieldName].classes[oFieldHash['class']] = 1;
					oUniqueFields[sFieldName].values[oFieldHash['value']] = 1;
				}
				else {
					var oClasses = {};
					oClasses[ oFieldHash['class'] ] = 1;
					var oValues = {};
					oValues[ oFieldHash['value'] ] = 1;
					oUniqueFields[sFieldName] = {
						count: 1,
						classes: oClasses,
						values: oValues
					}
				}
			}
		}
		
		var fieldSummaryDiv = document.createElement('div');
		fieldSummaryDiv.id = 'query_export_' + this.id + '_field_summary';
		headerContainerDiv.appendChild(fieldSummaryDiv);
		
		for (var sUniqueField in oUniqueFields){
			var oUniqueField = oUniqueFields[sUniqueField];
			var fieldNameLink = document.createElement('a');
			fieldNameLink.setAttribute('href', '#');
			fieldNameLink.innerHTML = sUniqueField + '(' + keys(oUniqueField.values).length + ')';
			fieldNameLink.id = 'individual_classes_menu_link_' + sUniqueField;
			fieldSummaryDiv.appendChild(fieldNameLink);
			var fieldNameLinkEl = new YAHOO.util.Element(fieldNameLink);
			fieldNameLinkEl.on('click', YAHOO.ELSA.groupData, [ YAHOO.ELSA.getLocalResultId(this.tab), null, sUniqueField ], this);
			fieldSummaryDiv.appendChild(document.createTextNode(' '));
		}
	};
};

YAHOO.ELSA.Results.Tabbed.Saved = function(p_oTabView, p_iQid){
	this.superClass = YAHOO.ELSA.Results.Tabbed;
	
	this.receiveResponse = function(oResponse){
		if (oResponse.responseText){
			var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
			if (typeof oReturn === 'object' && oReturn['error']){
				YAHOO.ELSA.Error(oReturn['error']);
			}
			else if (oReturn){
				//oQuery is this from the current scope
				var oSelf = oResponse.argument[0];
				logger.log(oReturn);
				oSelf.superClass(p_oTabView, oReturn['query_string'], 'Saved Query ' + p_iQid + ': ' + oReturn['query_string']);
				oSelf.loadResponse(oReturn);
			}
			else {
				logger.log(oReturn);
				YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
			}
		}
		else {
			YAHOO.ELSA.Error('No response text');
		}
	};
	
	// Get the saved query data from the server
	var request = YAHOO.util.Connect.asyncRequest('GET', 
			'Query/get_saved_result?qid=' + p_iQid,
			{ 
				success: this.receiveResponse,
				failure:function(oResponse){
					YAHOO.ELSA.Error('Query failed!'); tab.set('content', 'Error!'); return false;
				},
				argument: [this]
			}
	);
}

YAHOO.ELSA.Results.Tabbed.Live = function(p_oTabView, p_oQuery){
	this.superClass = YAHOO.ELSA.Results.Tabbed;
	try {
		this.superClass(p_oTabView, p_oQuery.stringifyTerms(), p_oQuery.stringifyTerms());
	}
	catch (e){
		logger.log('Tabbed.Live failed to create superclass: ', e);
		return false;
	}
	
	this.sentQuery = p_oQuery.toString(); //set this opaque string for later use
	
	/* Actually do the query */
	//logger.log('query obj:', p_oQuery);
	logger.log('sending query:' + this.sentQuery);//.toString());
	var request = YAHOO.util.Connect.asyncRequest('GET', 
			'Query/query?q=' + encodeURIComponent(this.sentQuery),//.toString()),
			{ 
				success:function(oResponse){
					var oRequest = oResponse.argument[0];
					logger.log('oRequest', oRequest);
					if (oResponse.responseText){
						var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
						if (typeof oReturn === 'object' && oReturn['error']){
							YAHOO.ELSA.Error(oReturn['error']);
							oRequest.closeTab(this);
						}
						else if (oReturn){
							recvQueryResults(oResponse);
						}
						else {
							logger.log(oReturn);
							YAHOO.ELSA.Error('Could not parse responseText: ' + oResponse.responseText);
							oRequest.closeTab(this);
						}
					}
					else {
						YAHOO.ELSA.Error('No response text');
						oRequest.closeTab(this);
					}
					
				}, 
				failure:function(oResponse){
					var oRequest = oResponse.argument[0];
					YAHOO.ELSA.Error('Query failed!'); 
					oRequest.closeTab(this); 
					return false;
				},
				argument: [this]
			}
	);
	
	var recvQueryResults = function(oResponse) {
		logger.log('recvQueryResults got results:', oResponse);
		var oSelf = oResponse.argument[0];
		logger.log('oQuery:', oSelf);
		try{
			if(oResponse.responseText !== undefined && oResponse.responseText){
				try{
					//logger.log('parsing: ', oResponse.responseText);
					var oSavedResults = YAHOO.lang.JSON.parse(oResponse.responseText);
					//logger.log('got results:', oSavedResults);
					oSelf.loadResponse(oSavedResults);
				}
				catch(e){
					logger.log('Could not parse response for form parameters because of an error: ', e.stack);
				}
			}
			else {
				logger.log('Did not receive query results for query id ' + oQuery.id);
			}
		}
		catch(e){
			logger.log('Error receiving query results:', e);
		}
	}
};


YAHOO.ELSA.getPreviousQueries = function(){
	if (!YAHOO.ELSA.previousQueriesDataSource){
		var formatMenu = function(elLiner, oRecord, oColumn, oData){
			// Create menu for our menu button
			var oButtonMenuCfg = [
				{ 
					text: 'Schedule', 
					value: 'schedule', 
					onclick:{
						fn: YAHOO.ELSA.scheduleQuery,
						obj: oRecord.getData().qid
					}
				},
				{ 
					text: 'Alert', 
					value: 'alert', 
					onclick:{
						fn: YAHOO.ELSA.createAlert,
						obj: oRecord.getData().qid
					}
				},
				{ 
					text: 'Open Ticket', 
					value: 'open_ticket', 
					onclick:{
						fn: YAHOO.ELSA.openTicket,
						obj: oRecord.getData().qid
					}
				}
			];
			
			var oButton = new YAHOO.widget.Button(
				{
					type:'menu', 
					label:'Actions',
					name: 'action_button_' + oRecord.getData().qid,
					menu: oButtonMenuCfg,
					container: elLiner
				});
		};
		YAHOO.ELSA.previousQueriesDataSource = new YAHOO.util.DataSource('Query/get_previous_queries?');
		YAHOO.ELSA.previousQueriesDataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
		YAHOO.ELSA.previousQueriesDataSource.responseSchema = {
			resultsList: "results",
			fields: ["qid", "query", "timestamp", "num_results", "milliseconds" ],
			metaFields: {
				totalRecords: 'totalRecords',
				recordsReturned: 'recordsReturned'
			}
		};
			
	}
	
	// Build the panel if necessary
	if (!YAHOO.ELSA.previousQueriesPanel){
		var oPanel = new YAHOO.ELSA.Panel('previous_queries');
		YAHOO.ELSA.previousQueriesPanel = oPanel.panel;
		YAHOO.ELSA.previousQueriesPanel.setHeader('Query History');
		
		YAHOO.ELSA.previousQueriesPanel.renderEvent.subscribe(function(){
			var myColumnDefs = [
				{ key:'menu', label:'Action', formatter:formatMenu },
				{ key:"qid", label:"QID", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
				{ key:"query", label:"Query", sortable:true },
				{ key:"timestamp", label:"Timestamp", editor:"date", formatter:YAHOO.ELSA.Query.formatDate, sortable:true },
				{ key:"num_results", label:"Results", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
				{ key:"milliseconds", label:"MS Taken", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true }
			];
			var oPaginator = new YAHOO.widget.Paginator({
			    pageLinks          : 10,
		        rowsPerPage        : 5,
		        rowsPerPageOptions : [5,20],
				template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
		        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
		    });
		    
		    var oDataTableCfg = {
		    	initialRequest: 'startIndex=0&results=5',
		    	dynamicData:true,
		    	sortedBy : {key:"qid", dir:YAHOO.widget.DataTable.CLASS_DESC},
		    	paginator: oPaginator
		    };
		    var dtDiv = document.createElement('div');
			dtDiv.id = 'previous_queries_dt';
			document.body.appendChild(dtDiv);
			try {	
				YAHOO.ELSA.prevSearchDT = new YAHOO.widget.DataTable(dtDiv, 
					myColumnDefs, YAHOO.ELSA.previousQueriesDataSource, oDataTableCfg );
				YAHOO.ELSA.prevSearchDT.handleDataReturnPayload = function(oRequest, oResponse, oPayload){
					oPayload.totalRecords = oResponse.meta.totalRecords;
					return oPayload;
				}
				YAHOO.ELSA.previousQueriesPanel.setBody(dtDiv);
			}
			catch (e){
				logger.log('Error:', e);
			}
		});
	}
	
	YAHOO.ELSA.previousQueriesPanel.render();
	YAHOO.ELSA.previousQueriesPanel.show();
};

YAHOO.ELSA.deleteScheduledQuery = function(p_sType, p_aArgs, p_iQid){
	
};

YAHOO.ELSA.scheduleQuery = function(p_sType, p_aArgs, p_iQid){
	
	if (!YAHOO.ELSA.scheduleQueryDialog){
		var handleSubmit = function(){
			this.submit();
		};
		var handleCancel = function(){
			this.hide();
		};
		var oPanel = new YAHOO.ELSA.Panel('schedule_query', {
			buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
				{ text:"Cancel", handler:handleCancel } ]
		});
		YAHOO.ELSA.scheduleQueryDialog = oPanel.panel;
		var handleSuccess = function(p_oResponse){
			var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
			if (response['error']){
				YAHOO.ELSA.Error(response['error']);
			}
			else {
				YAHOO.ELSA.getQuerySchedule();
				logger.log('successful submission');
			}
		};
		YAHOO.ELSA.scheduleQueryDialog.callback = {
			success: handleSuccess,
			failure: YAHOO.ELSA.Error
		};
		YAHOO.ELSA.scheduleQueryDialog.validate = function(){
			if (!this.getData().count || !parseInt(this.getData().count)){
				YAHOO.ELSA.Error('Need a valid integer as an interval');
				return false;
			}
			if (!this.getData().time_unit || !parseInt(this.getData().time_unit)){
				YAHOO.ELSA.Error('Please select a time unit');
				return false;
			}
			if (!(parseInt(this.getData().days) >= 0)){
				YAHOO.ELSA.Error('Please enter a valid number of days to run for.');
				return false;
			}
			return true;
		}
	
		var onIntervalMenuItemClick = function(p_sType, p_aArgs, p_oItem){
			var sText = p_oItem.cfg.getProperty("text");
			// Set the label of the button to be our selection
			var oIntervalButton = YAHOO.widget.Button.getButton('interval_select_button');
			oIntervalButton.set('label', sText);
			var oFormEl = YAHOO.util.Dom.get('interval_select_form');
			var oInputEl = YAHOO.util.Dom.get('schedule_input_interval_unit');
			if (oInputEl){
				oInputEl.setAttribute('value', p_oItem.value);
			}
			else {
				var oInputEl = document.createElement('input');
				oInputEl.id = 'schedule_input_interval_unit';
				oInputEl.setAttribute('type', 'hidden');
				oInputEl.setAttribute('name', 'time_unit');
				oInputEl.setAttribute('value', p_oItem.value);
				oFormEl.appendChild(oInputEl);
			}
		}
		
		//	Create an array of YAHOO.widget.MenuItem configuration properties
		var oIntervalMenuSources = [ 
			{text:'Minute', value:'6', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Hour', value:'5', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Day', value:'4', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Week', value:'3', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Month', value:'2', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Year', value:'1', onclick: { fn: onIntervalMenuItemClick }}
		];
		
		var oIntervalMenuButtonCfg = {
			id: 'interval_select_button',
			type: 'menu',
			label: 'Time Unit',
			name: 'interval_select_button',
			menu: oIntervalMenuSources
		};
		
		var action_id = 0;
		for (var i in YAHOO.ELSA.formParams.schedule_actions){
			if (YAHOO.ELSA.formParams.schedule_actions[i].action === 'Save'){
				action_id = YAHOO.ELSA.formParams.schedule_actions[i].action_id;
				break;
			}
		}
		
		var oFormGridCfg = {
			form_attrs:{
				action: 'Query/schedule_query',
				method: 'POST',
				id: 'interval_select_form'
			},
			grid: [
				[ {type:'text', args:'Run every'}, {type:'input', args:{id:'schedule_input_interval_count', name:'count', size:2}}, {type:'widget', className:'Button', args:oIntervalMenuButtonCfg} ],
				[ {type:'text', args:'Days to run'},  {type:'input', args:{id:'schedule_input_start_date', name:'days', value:7, size:2}}, {type:'text', args:'(enter 0 for forever)'} ],
				[ {type:'input', args:{type:'hidden', id:'schedule_input_qid', name:'qid', value:p_iQid}}, {type:'input', args:{type:'hidden', id:'schedule_input_action', name:'action_id', value:action_id}} ]
			]
		};
		YAHOO.ELSA.scheduleQueryDialog.setHeader('Schedule');
		YAHOO.ELSA.scheduleQueryDialog.setBody('');
		// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
		YAHOO.ELSA.scheduleQueryDialog.render();
		
		// Now build a new form using the element auto-generated by widget.Dialog
		var oForm = new YAHOO.ELSA.Form(YAHOO.ELSA.scheduleQueryDialog.form, oFormGridCfg);
	}
	else {
		// update with the given qid
		YAHOO.util.Dom.get('schedule_input_qid').value = p_iQid;
	}
	
	YAHOO.ELSA.scheduleQueryDialog.show();
	YAHOO.ELSA.scheduleQueryDialog.bringToTop();
}

YAHOO.ELSA.saveResults = function(p_sType, p_aArgs, p_iId){
	logger.log('p_iId:', p_iId);
	var iLocalResultId = YAHOO.ELSA.getLocalResultIdFromQueryId(p_iId);
	logger.log('localResultId: ' + iLocalResultId);
	
	var handleSubmit = function(p_sType, p_oDialog){
		var sComments = YAHOO.util.Dom.get('save_results_input').value;
		logger.log('saving comments: ' + sComments + ' for qid ' + p_iId);
		YAHOO.ELSA.localResults[iLocalResultId].save(sComments);
		this.hide();
	};
	var handleCancel = function(){
		this.hide();
	};
	var dialogDiv = document.createElement('div');
	dialogDiv.id = 'save_results';
	document.body.appendChild(dialogDiv);
	YAHOO.ELSA.saveResultsDialog = new YAHOO.widget.Dialog(dialogDiv, {
		underlay: 'none',
		visible:true,
		fixedcenter:true,
		draggable:true,
		buttons : [ { text:"Submit", handler:{ fn:handleSubmit }, isDefault:true },
			{ text:"Cancel", handler:handleCancel } ]
	});
	
	YAHOO.ELSA.saveResultsDialog.validate = function(){
		return true;
	}
		
	var oFormGridCfg = {
		form_attrs:{
			id: 'save_results_form'
		},
		grid: [
			[ {type:'text', args:'Comment'}, {type:'input', args:{id:'save_results_input', name:'comment', size:80}} ]
		]
	};

	// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
	YAHOO.ELSA.saveResultsDialog.setHeader('Save Results');
	YAHOO.ELSA.saveResultsDialog.setBody('');
	YAHOO.ELSA.saveResultsDialog.render();
	
	// Now build a new form using the element auto-generated by widget.Dialog
	var oForm = new YAHOO.ELSA.Form(YAHOO.ELSA.saveResultsDialog.form, oFormGridCfg);
	YAHOO.ELSA.saveResultsDialog.show();
};

YAHOO.ELSA.exportResults = function(p_sType, p_aArgs, p_iId){
	logger.log('p_iId:', p_iId);
	YAHOO.ELSA.exportResults.id = YAHOO.ELSA.getLocalResultIdFromQueryId(p_iId);
	YAHOO.ELSA.exportResults.plugin = '';
	if (!YAHOO.ELSA.exportResultsDialog){
		var handleSubmit = function(p_sType, p_oDialog){
			logger.log('exporting results for query.id ' + YAHOO.ELSA.exportResults.id + ' with method ' + YAHOO.ELSA.exportResults.plugin);
			YAHOO.ELSA.localResults[YAHOO.ELSA.exportResults.id].send(YAHOO.ELSA.exportResults.plugin, 'Query/export');
			this.hide();
		};
		var handleCancel = function(){
			this.hide();
		};
		var dialogDiv = document.createElement('div');
		dialogDiv.id = 'export_results';
		document.body.appendChild(dialogDiv);
		YAHOO.ELSA.exportResultsDialog = new YAHOO.widget.Dialog(dialogDiv, {
			underlay: 'none',
			//zIndex: 3,
			visible:true,
			fixedcenter:true,
			draggable:true,
			buttons : [ { text:"Submit", handler:{ fn:handleSubmit }, isDefault:true },
				{ text:"Cancel", handler:handleCancel } ]
		});
		
		YAHOO.ELSA.exportResultsDialog.validate = function(){
			return true;
		}
		
	}
	
	var oButton;
	//	"click" event handler for each item in the Button's menu
	var onMenuItemClick = function(p_sType, p_aArgs, p_oItem){
		var sText = p_oItem.cfg.getProperty("text");
		// Set the label of the button to be our selection
		oButton.set('label', sText);
		YAHOO.ELSA.exportResults.plugin = p_oItem.value;
	}
	
	//	Create an array of YAHOO.widget.MenuItem configuration properties
	var oMenuSources = [
		{text:'Excel', value:'Spreadsheet', onclick: { fn: onMenuItemClick }},
		{text:'PDF', value:'PDF', onclick: { fn: onMenuItemClick }},
		{text:'CSV', value:'CSV', onclick: { fn: onMenuItemClick }},
		{text:'HTML', value:'HTML', onclick: { fn: onMenuItemClick }},
		{text:'HTTP Request Tree', value:'HTTPRequestTree', onclick: { fn: onMenuItemClick }}
	];
	
	var oMenuButtonCfg = {
		type: 'menu',
		label: 'Export As...',
		name: 'export_select_button',
		menu: oMenuSources
	};
	
	var menuButtonCallback = function(p_oArgs, p_oWidget, p_oEl){
		// Set this oButton since we apparently can't get it via parent.parent later in MenuItem
		oButton = p_oWidget;
	}
	
	var oFormGridCfg = {
		form_attrs:{
			id: 'export_results_form'
		},
		grid: [
			[ {type:'text', args:'Format to export data'}, {type:'widget', className:'Button', args:oMenuButtonCfg, callback:menuButtonCallback} ]
		]
	};

	// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
	YAHOO.ELSA.exportResultsDialog.setHeader('Export Results');
	YAHOO.ELSA.exportResultsDialog.setBody('');
	YAHOO.ELSA.exportResultsDialog.render();
	
	// Now build a new form using the element auto-generated by widget.Dialog
	var oForm = new YAHOO.ELSA.Form(YAHOO.ELSA.exportResultsDialog.form, oFormGridCfg);
	YAHOO.ELSA.exportResultsDialog.show();
};

YAHOO.ELSA.exportData = function(p_sType, p_aArgs, p_oData){
	YAHOO.ELSA.exportData.data = p_oData;
	if (!YAHOO.ELSA.exportDataDialog){
		var handleSubmit = function(p_sType, p_oDialog){
			logger.log('exporting data with method ' + YAHOO.ELSA.exportData.plugin, YAHOO.ELSA.exportData.data);
			YAHOO.ELSA.send(YAHOO.ELSA.exportData.plugin, 'Query/export', YAHOO.ELSA.exportData.data);
			this.hide();
		};
		var handleCancel = function(){
			this.hide();
		};
		var dialogDiv = document.createElement('div');
		dialogDiv.id = 'export_data';
		document.body.appendChild(dialogDiv);
		YAHOO.ELSA.exportDataDialog = new YAHOO.widget.Dialog(dialogDiv, {
			underlay: 'none',
			visible:true,
			fixedcenter:true,
			draggable:true,
			buttons : [ { text:"Submit", handler:{ fn:handleSubmit }, isDefault:true },
				{ text:"Cancel", handler:handleCancel } ]
		});
		
		YAHOO.ELSA.exportDataDialog.validate = function(){
			return true;
		}
		
	}
	
	//	"click" event handler for each item in the Button's menu
	var onMenuItemClick = function(p_sType, p_aArgs, p_oItem){
		var sText = p_oItem.cfg.getProperty("text");
		// Set the label of the button to be our selection
		var oButton = YAHOO.widget.Button.getButton('export_select_button');
		oButton.set('label', sText);
		YAHOO.ELSA.exportData.plugin = p_oItem.value;
	}
	
	//	Create an array of YAHOO.widget.MenuItem configuration properties
	var oMenuSources = [
		{text:'Excel', value:'Spreadsheet', onclick: { fn: onMenuItemClick }},
		{text:'PDF', value:'PDF', onclick: { fn: onMenuItemClick }},
		{text:'CSV', value:'CSV', onclick: { fn: onMenuItemClick }},
		{text:'HTML', value:'HTML', onclick: { fn: onMenuItemClick }}
	];
	
	var oMenuButtonCfg = {
		id: 'export_select_button',
		type: 'menu',
		label: 'Export As...',
		name: 'export_select_button',
		menu: oMenuSources
	};
	
	var oFormGridCfg = {
		form_attrs:{
			id: 'export_results_form'
		},
		grid: [
			[ {type:'text', args:'Format to export data'}, {type:'widget', className:'Button', args:oMenuButtonCfg} ]
		]
	};

	// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
	YAHOO.ELSA.exportDataDialog.setHeader('Export Data');
	YAHOO.ELSA.exportDataDialog.setBody('');
	YAHOO.ELSA.exportDataDialog.render();
	
	// Now build a new form using the element auto-generated by widget.Dialog
	var oForm = new YAHOO.ELSA.Form(YAHOO.ELSA.exportDataDialog.form, oFormGridCfg);
	YAHOO.ELSA.exportDataDialog.show();
};

YAHOO.ELSA.Error = function(p_sError){
	logger.log('got error', p_sError);
	var oNotificationPanel = new YAHOO.ELSA.Panel('error', {visible:true, modal:true});
	var oEl = new YAHOO.util.Element(oNotificationPanel.panel.header);
	oEl.addClass('error');
	oNotificationPanel.panel.setBody(p_sError);
	oNotificationPanel.panel.setHeader('Error');
	oNotificationPanel.panel.render();
	oNotificationPanel.panel.show();
};

YAHOO.ELSA.getSavedQueries = function(){
	if (!YAHOO.ELSA.getSavedQueries.dataSource){
		var formatMenu = function(elLiner, oRecord, oColumn, oData){
			// Create menu for our menu button
			var oButtonMenuCfg = [
				{ 
					text: 'Get Results', 
					value: 'get', 
					onclick:{
						fn: YAHOO.ELSA.getSavedResult,
						obj: oRecord.getData().qid
					}
				},
				{ 
					text: 'Schedule', 
					value: 'schedule', 
					onclick:{
						fn: YAHOO.ELSA.scheduleQuery,
						obj: oRecord.getData().qid
					}
				},
				{ 
					text: 'Alert', 
					value: 'alert', 
					onclick:{
						fn: YAHOO.ELSA.createAlert,
						obj: oRecord.getData().qid
					}
				},
				{ 
					text: 'Open Ticket', 
					value: 'open_ticket', 
					onclick:{
						fn: YAHOO.ELSA.openTicket,
						obj: oRecord.getData().qid
					}
				},
				{ 
					text: 'Delete', 
					value: 'delete', 
					onclick:{
						fn: function(p_sType, p_aArgs, p_iQid){
							oSavedQuery = new YAHOO.ELSA.Results.Saved(p_iQid);
							oSavedQuery.remove();
						},
						obj: oRecord.getData().qid
					}
				}
			];
			
			var oButton = new YAHOO.widget.Button(
				{
					type:'menu', 
					label:'Actions',
					name: 'action_button_' + oRecord.getData().qid,
					menu: oButtonMenuCfg,
					container: elLiner
				});
		};
		YAHOO.ELSA.getSavedQueries.dataSource = new YAHOO.util.DataSource('Query/get_saved_queries?');
		YAHOO.ELSA.getSavedQueries.dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
		YAHOO.ELSA.getSavedQueries.dataSource.responseSchema = {
			resultsList: "results",
			fields: ["qid", "query", "timestamp", "num_results", "comments", "hash" ],
			metaFields: {
				totalRecords: 'totalRecords',
				recordsReturned: 'recordsReturned'
			}
		};
			
	}
	
	// Build the panel if necessary
	if (!YAHOO.ELSA.getSavedQueries.panel){
		var oPanel = new YAHOO.ELSA.Panel('saved_queries');
		YAHOO.ELSA.getSavedQueries.panel = oPanel.panel;
		YAHOO.ELSA.getSavedQueries.panel.setHeader('Saved Queries');
		
		var formatPermaLink = function(elLiner, oRecord, oColumn, oData){
			elLiner.innerHTML = '<a href="get_results?qid=' + oRecord.getData().qid + '&hash=' + oRecord.getData().hash + '">permalink</a>';
		}
		
		YAHOO.ELSA.getSavedQueries.panel.renderEvent.subscribe(function(){
			var myColumnDefs = [
				{ key:'menu', label:'Action', formatter:formatMenu },
				{ key:"qid", label:"QID", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
				{ key:"query", label:"Query", sortable:true },
				{ key:"timestamp", label:"Timestamp", editor:"date", formatter:YAHOO.ELSA.formatDateFromUnixTime, sortable:true },
				{ key:"num_results", label:"Results", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
				{ key:"comments", label:"Comments", sortable:true },
				{ key:"permalink", label:"Permalink", formatter:formatPermaLink }
			];
			var oPaginator = new YAHOO.widget.Paginator({
			    pageLinks          : 10,
		        rowsPerPage        : 5,
		        rowsPerPageOptions : [5,20],
		        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
		        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
		    });
		    
		    var oDataTableCfg = {
		    	initialRequest: 'startIndex=0&results=5',
		    	initialLoad: true,
		    	dynamicData: true,
		    	sortedBy : {key:"qid", dir:YAHOO.widget.DataTable.CLASS_DESC},
		    	paginator: oPaginator //,
		    	//MSG_EMPTY: 'Loading...'
		    };
		    var dtDiv = document.createElement('div');
			dtDiv.id = 'saved_queries_dt';
			document.body.appendChild(dtDiv);
			try {	
				YAHOO.ELSA.getSavedQueries.dataTable = new YAHOO.widget.DataTable(dtDiv, 
					myColumnDefs, YAHOO.ELSA.getSavedQueries.dataSource, oDataTableCfg );
				YAHOO.ELSA.getSavedQueries.dataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload){
					oPayload.totalRecords = oResponse.meta.totalRecords;
					return oPayload;
				}
				YAHOO.ELSA.getSavedQueries.panel.setBody(dtDiv);
			}
			catch (e){
				logger.log('Error:', e);
			}
		});
	}
	YAHOO.ELSA.getSavedQueries.panel.render();
	YAHOO.ELSA.getSavedQueries.panel.show();
};

YAHOO.ELSA.getQuerySchedule = function(){
	if (!YAHOO.ELSA.getQuerySchedule.dataSource){
		var deleteScheduledQuery = function(p_sType, p_aArgs, p_oRecord){
			oQuery = new YAHOO.ELSA.Query.Scheduled(p_oRecord);
			oQuery.remove();
		};
		var formatMenu = function(elLiner, oRecord, oColumn, oData){
			// Create menu for our menu button
			var oButtonMenuCfg = [
				{ 
					text: 'Delete', 
					value: 'delete', 
					onclick:{
						fn: deleteScheduledQuery,
						obj: oRecord
					}
				}
			];
			
			var oButton = new YAHOO.widget.Button(
				{
					type:'menu', 
					label:'Actions',
					menu: oButtonMenuCfg,
					container: elLiner
				});
		};
		YAHOO.ELSA.getQuerySchedule.dataSource = new YAHOO.util.DataSource('Query/get_scheduled_queries?');
		YAHOO.ELSA.getQuerySchedule.dataSource.responseType = YAHOO.util.DataSource.TYPE_JSON;
		YAHOO.ELSA.getQuerySchedule.dataSource.responseSchema = {
			resultsList: 'results',
			fields: ['id', 'query', 'frequency', 'start', 'end', 'action', 'action_params', 'enabled', 'last_alert', 'alert_threshold' ],
			metaFields: {
				totalRecords: 'totalRecords',
				recordsReturned: 'recordsReturned'
			}
		};
			
	}
	else {
		// we need fresh data!
		logger.log('refreshing data table');
		YAHOO.ELSA.getQuerySchedule.dataTable.initializeTable();
	}
	
	YAHOO.ELSA.getQuerySchedule.dropRow = function(p_iRecordSetId){
		logger.log('deleting recordset id ' + p_iRecordSetId);
		YAHOO.ELSA.getQuerySchedule.dataTable.deleteRow(p_iRecordSetId);
	};
	
	// Build the panel if necessary
	if (!YAHOO.ELSA.getQuerySchedule.panel){
		var oPanel = new YAHOO.ELSA.Panel('query_schedule');
		YAHOO.ELSA.getQuerySchedule.panel = oPanel.panel;
		
		var makeFrequency = function(p_i){
			var ret = [];
			for (var i = 1; i <=7; i++){
				if (i == p_i){
					ret.push(1);
				}
				else {
					ret.push(0);
				}
			}
			return ret.join(':');
		};
		
		var aIntervalValues = [
			{ label:'Year', value: makeFrequency(1) },
			{ label:'Month', value: makeFrequency(2) },
			{ label:'Week', value: makeFrequency(3) },
			{ label:'Day', value: makeFrequency(4) },
			{ label:'Hour', value: makeFrequency(5) },
			{ label:'Minute', value: makeFrequency(6) },
			{ label:'Second', value: makeFrequency(7) }
		];
		
		var formatInterval = function(elLiner, oRecord, oColumn, oData){
			var aTimeUnits = oData.split(':');
			
			for (var i = 0; i < aTimeUnits.length; i++){
				if (aTimeUnits[i] == 1){
					elLiner.innerHTML = aIntervalValues[i]['label'];
					logger.log('setting interval: ' + aIntervalValues[i]['label']);
				}
			}
		};
		
		var aEnabledValues = [
			{ label: 'Disabled', value: 0 },
			{ label: 'Enabled', value: 1 }
		];
		
		var formatEnabled = function(elLiner, oRecord, oColumn, oData){
			var i = parseInt(oData);
			if (!i){
				i = 0;
			}
			elLiner.innerHTML = aEnabledValues[i]['label'];
		};
		
		var formatQuery  = function(elLiner, oRecord, oColumn, oData){
			try {
				oParsed = YAHOO.lang.JSON.parse(oData);
				elLiner.innerHTML = oParsed['query_string'];
			}
			catch (e){
				logger.log(e);
				elLiner.innerHTML = '';
			}
		};
		
		var aActions = [];
		for (var i = 0; i < YAHOO.ELSA.formParams.schedule_actions.length; i++){
			aActions.push({ label: YAHOO.ELSA.formParams.schedule_actions[i]['action'], value: YAHOO.ELSA.formParams.schedule_actions[i]['action_id'] } );
		}
		var formatActions = function(elLiner, oRecord, oColumn, oData){
			// we will accept either the string label or an int
			var p_i = parseInt(oData);
			if (!p_i){
				elLiner.innerHTML = oData;
			}
			else {
				for (var i = 0; i < aActions.length; i++){
					if (aActions[i]['value'] == p_i){
						elLiner.innerHTML = aActions[i]['label'];
					}
				}
			}
		};
		
		var formatThreshold = function(elLiner, oRecord, oColumn, oData){
			var p_i = parseInt(oData);
			logger.log('oData', oData);
			logger.log('oColumn', oColumn);
			logger.log('oRecord', oRecord);
			if (!p_i){
				elLiner.innerHTML = oData;
			}
			else {
				if (p_i >= 86400){
					elLiner.innerHTML = (p_i / 86400) + ' days';
				}
				else if (p_i >= 3600){
					elLiner.innerHTML = (p_i / 3600) + ' hours';
				}
				else if (p_i >= 60){
					elLiner.innerHTML = (p_i / 60) + ' minutes';
				}
				else {
					elLiner.innerHTML = p_i + ' seconds';	
				}
			}
		}
		
		YAHOO.ELSA.getQuerySchedule.panel.setHeader('Scheduled Queries');
		
		var asyncSubmitter = function(p_fnCallback, p_oNewValue){
			// called in the scope of the editor
			logger.log('editor this: ', this);
			logger.log('p_oNewValue:', p_oNewValue);
			
			var oRecord = this.getRecord(),
				oColumn = this.getColumn(),
				sOldValue = this.value,
				oDatatable = this.getDataTable();
			logger.log('column:', oColumn);
			
			var oQuery = new YAHOO.ELSA.Query.Scheduled(oRecord);
			logger.log('oQuery:', oQuery);
			oQuery.set(oColumn.field, p_oNewValue); //will call the asyncSubmitterCallback
		};
		
		YAHOO.ELSA.getQuerySchedule.asyncSubmitterCallback = function(p_bSuccess, p_oNewValue){
			logger.log('arguments:', arguments);
			logger.log('callback p_bSuccess', p_bSuccess);
			logger.log('callback p_oNewValue:', p_oNewValue);
			if (p_bSuccess){
				logger.log('setting ' + YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().getColumn().field + ' to ' + p_oNewValue);
				YAHOO.ELSA.getQuerySchedule.dataTable.updateCell(
					YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().getRecord(),
					YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().getColumn(),
					p_oNewValue
				);
			}
			YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().unblock();
			YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().cancel(); //hides box
		};
		
		// Set up editing flow
		var highlightEditableCell = function(p_oArgs) {
			var elCell = p_oArgs.target;
			if(YAHOO.util.Dom.hasClass(elCell, "yui-dt-editable")) {
				this.highlightCell(elCell);
			}
		};
		
		YAHOO.ELSA.getQuerySchedule.cellEditorValidatorInt = function(p_sInputValue, p_sCurrentValue, p_oEditorInstance){
			return parseInt(p_sInputValue);
		};
		
		YAHOO.ELSA.getQuerySchedule.cellEditorValidatorJSON = function(p_sInputValue, p_sCurrentValue, p_oEditorInstance){
			try {
				return YAHOO.lang.JSON.parse(p_sInputValue);
			}
			catch (e){
				YAHOO.ELSA.Error(e);
				return p_sCurrentValue;
			}
		};
		
		YAHOO.ELSA.getQuerySchedule.cellEditorValidatorQuery = function(p_sInputValue, p_sCurrentValue, p_oEditorInstance){
			var oQueryParams;
			try {
				oQueryParams = YAHOO.lang.JSON.parse(p_sInputValue);
			}
			catch (e){
				YAHOO.ELSA.Error(e);
				return;
			}
			logger.log('query_string:', typeof oQueryParams['query_string']);
			if (!oQueryParams['query_string'] || typeof oQueryParams['query_meta_params'] != 'object'){
				YAHOO.ELSA.Error('Need query_string and query_meta_params in obj');
				return;
			}
			return oQueryParams;
		};
		
		YAHOO.ELSA.getQuerySchedule.onEventShowCellEditor = function(p_oArgs){
			logger.log('p_oArgs', p_oArgs);
			this.onEventShowCellEditor(p_oArgs);
			logger.log('YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor():',YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor());
			// increase the size of the textbox, if we have one
			if (YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor() && YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().textbox){				
				YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().textbox.setAttribute('size', 20);
				YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().textbox.removeAttribute('style');
				// create key listener for the submit
				var enterKeyListener = new YAHOO.util.KeyListener(
						YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().textbox,
						{ keys: 13 },
						{ 	fn: function(eName, p_aArgs){
								var oEvent = p_aArgs[1];
								// Make sure we don't submit the form
								YAHOO.util.Event.stopEvent(oEvent);
								var tgt=(oEvent.target ? oEvent.target : 
									(oEvent.srcElement ? oEvent.srcElement : null)); 
								try{
									tgt.blur();
								}
								catch(e){}
								var op = '=';
								YAHOO.ELSA.getQuerySchedule.dataTable.getCellEditor().save();
							},
							scope: YAHOO.ELSA,
							correctScope: false
						}
				);
				enterKeyListener.enable();
			}
		}
		
		YAHOO.ELSA.getQuerySchedule.panel.renderEvent.subscribe(function(){
			var myColumnDefs = [
				{ key:'menu', label:'Action', formatter:formatMenu },
				{ key:"id", label:"ID", formatter:YAHOO.widget.DataTable.formatNumber, sortable:true },
				{ key:"query", label:"Query", formatter:formatQuery, sortable:true, editor: new YAHOO.widget.TextboxCellEditor({asyncSubmitter:asyncSubmitter, validator:YAHOO.ELSA.getQuerySchedule.cellEditorValidatorQuery}) },
				{ key:'frequency', label:'Interval', formatter:formatInterval, sortable:true, editor: new YAHOO.widget.DropdownCellEditor({asyncSubmitter:asyncSubmitter, dropdownOptions:aIntervalValues}) },
				{ key:'start', label:'Starts On', formatter:YAHOO.ELSA.formatDateFromUnixTime, sortable:true, editor: new YAHOO.widget.DateCellEditor({asyncSubmitter:asyncSubmitter}) },
				{ key:'end', label:'Ends On', formatter:YAHOO.ELSA.formatDateFromUnixTime, sortable:true, editor: new YAHOO.widget.DateCellEditor({asyncSubmitter:asyncSubmitter}) },
				{ key:'action', label:'Action', formatter:formatActions, sortable:true, editor: new YAHOO.widget.DropdownCellEditor({asyncSubmitter:asyncSubmitter, dropdownOptions:aActions}) },
				{ key:'enabled', label:'Enabled', formatter:formatEnabled, sortable:true, editor: new YAHOO.widget.DropdownCellEditor({asyncSubmitter:asyncSubmitter, dropdownOptions:aEnabledValues}) },
				{ key:'last_alert', label:'Last Alert', formatter:YAHOO.ELSA.formatDateFromUnixTime, sortable:true },
				{ key:'alert_threshold', label:'Alert Threshold', formatter:formatThreshold, editor: new YAHOO.widget.TextboxCellEditor({asyncSubmitter:asyncSubmitter, validator:YAHOO.ELSA.getQuerySchedule.cellEditorValidatorInt}) }
			];
			var oPaginator = new YAHOO.widget.Paginator({
			    pageLinks          : 10,
		        rowsPerPage        : 5,
		        rowsPerPageOptions : [5,20],
		        template           : "{CurrentPageReport} {PreviousPageLink} {PageLinks} {NextPageLink} {RowsPerPageDropdown}",
		        pageReportTemplate : "<strong>Records: {totalRecords} </strong> "
		    });
		    
		    var oDataTableCfg = {
		    	initialRequest: 'startIndex=0&results=5',
		    	initialLoad: true,
		    	dynamicData: true,
		    	sortedBy : {key:"id", dir:YAHOO.widget.DataTable.CLASS_DESC},
		    	paginator: oPaginator
		    };
		    var dtDiv = document.createElement('div');
			dtDiv.id = 'saved_queries_dt';
			document.body.appendChild(dtDiv);
			YAHOO.ELSA.getQuerySchedule.dataTable = '';
			try {	
				YAHOO.ELSA.getQuerySchedule.dataTable = new YAHOO.widget.DataTable(dtDiv, 
					myColumnDefs, YAHOO.ELSA.getQuerySchedule.dataSource, oDataTableCfg );
				logger.log(YAHOO.ELSA.getQuerySchedule.dataSource);
				logger.log(YAHOO.ELSA.getQuerySchedule.dataTable);
				YAHOO.ELSA.getQuerySchedule.dataTable.handleDataReturnPayload = function(oRequest, oResponse, oPayload){
					oPayload.totalRecords = oResponse.meta.totalRecords;
					return oPayload;
				}
				
				YAHOO.ELSA.getQuerySchedule.dataTable.subscribe("cellClickEvent", 
					YAHOO.ELSA.getQuerySchedule.onEventShowCellEditor);
				YAHOO.ELSA.getQuerySchedule.panel.setBody(dtDiv);
			}
			catch (e){
				logger.log('Error:', e);
			}
		});
	}
	
	YAHOO.ELSA.getQuerySchedule.panel.render();
	YAHOO.ELSA.getQuerySchedule.panel.show();
};

YAHOO.ELSA.formatDateFromUnixTime = function(p_elCell, oRecord, oColumn, p_oData)
{
	logger.log('p_oData', p_oData);
	var oDate = p_oData;
	if(p_oData instanceof Date){
	}
	else {
		var mSec = p_oData * 1000;
		oDate = new Date();
		oDate.setTime(mSec);
	}
	p_elCell.innerHTML = oDate.toString();
	oRecord.setData(oColumn.key, oDate);
};

YAHOO.ELSA.getSavedResult = function(p_sType, p_aArgs, p_iQid){
	var oSavedResults = new YAHOO.ELSA.Results.Tabbed.Saved(YAHOO.ELSA.tabView, p_iQid);
};

YAHOO.ELSA.createAlert = function(p_sType, p_aArgs, p_iQid){
	if (!YAHOO.ELSA.createAlertDialog){
		var action_id = 0;
		for (var i in YAHOO.ELSA.formParams.schedule_actions){
			if (YAHOO.ELSA.formParams.schedule_actions[i].action === 'Email'){
				action_id = YAHOO.ELSA.formParams.schedule_actions[i].action_id;
				break;
			}
		}
		
		var handleSubmit = function(){
			this.submit();
		};
		var handleCancel = function(){
			this.hide();
		};
		var handleSuccess = function(p_oResponse){
			var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
			if (response['error']){
				YAHOO.ELSA.Error(response['error']);
			}
			else {
				YAHOO.ELSA.getQuerySchedule();
				logger.log('successful submission');
			}
		};
		
		var oPanel = new YAHOO.ELSA.Panel('create_alert', {
			underlay: 'none',
			buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
				{ text:"Cancel", handler:handleCancel } ]
		});
		YAHOO.ELSA.createAlertDialog = oPanel.panel;
		
		YAHOO.ELSA.createAlertDialog.callback = {
			success: handleSuccess,
			failure: YAHOO.ELSA.Error
		};
		YAHOO.ELSA.createAlertDialog.validate = function(){
			if (!this.getData().threshold_count || !parseInt(this.getData().threshold_count)){
				YAHOO.ELSA.Error('Need a valid integer as an interval');
				return false;
			}
			if (!this.getData().threshold_time_unit || !parseInt(this.getData().threshold_time_unit)){
				YAHOO.ELSA.Error('Please select a time unit');
				return false;
			}
			return true;
		}
	
		//	"click" event handler for each item in the Button's menu
		var onIntervalMenuItemClick = function(p_sType, p_aArgs, p_oItem){
			var oIntervalButton = YAHOO.widget.Button.getButton('create_alert_interval_select_button');
			var sText = p_oItem.cfg.getProperty("text");
			// Set the label of the button to be our selection
			oIntervalButton.set('label', sText);
			var oInputEl = YAHOO.util.Dom.get('create_alert_input_interval_unit');
			if (oInputEl){
				oInputEl.setAttribute('value', p_oItem.value);
			}
			else {
				oInputEl = document.createElement('input');
				oInputEl.id = 'create_alert_input_interval_unit';
				oInputEl.setAttribute('type', 'hidden');
				oInputEl.setAttribute('name', 'threshold_time_unit');
				oInputEl.setAttribute('value', p_oItem.value);
				oFormEl.appendChild(oInputEl);
			}
		}
		
		//	Create an array of YAHOO.widget.MenuItem configuration properties
		var aIntervalMenuSources = [ 
			{text:'Minutes', value:'6', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Hours', value:'5', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Days', value:'4', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Weeks', value:'3', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Months', value:'2', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Years', value:'1', onclick: { fn: onIntervalMenuItemClick }}
		];
		
		var oIntervalMenuButtonCfg = {
			id: 'create_alert_interval_select_button',
			type: 'menu',
			label: 'Time Unit',
			name: 'create_alert_interval_select_button',
			menu: aIntervalMenuSources
		};
		
		var oFormGridCfg = {
			form_attrs:{
				action: 'Query/schedule_query',
				method: 'POST',
				id: 'create_alert_form'
			},
			grid: [
				[ {type:'text', args:'Alert no more than once every '}, {type:'input', args:{id:'create_alert_input_interval_count', name:'threshold_count', size:2}}, {type:'widget', className:'Button', args:oIntervalMenuButtonCfg} ],
				[ {type:'input', args:{type:'hidden', id:'create_alert_input_qid', name:'qid', value:p_iQid}} ]
			]
		};
		YAHOO.ELSA.createAlertDialog.setHeader('Create Alert');
		YAHOO.ELSA.createAlertDialog.setBody('');
		// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
		YAHOO.ELSA.createAlertDialog.render();
		
		// Now build a new form using the element auto-generated by widget.Dialog
		var oForm = new YAHOO.ELSA.Form(YAHOO.ELSA.createAlertDialog.form, oFormGridCfg);
		// Static hidden form values
		var oFormEl = YAHOO.util.Dom.get('create_alert_form');
		var oDaysEl = YAHOO.util.Dom.get('create_alert_input_days');
		if (!oDaysEl){
			oDaysEl = document.createElement('input');
			oDaysEl.id = 'create_alert_input_days';
			oDaysEl.setAttribute('type', 'hidden');
			oDaysEl.setAttribute('name', 'days');
			oDaysEl.setAttribute('value', 0);
			oFormEl.appendChild(oDaysEl);
		}
		
		var oActionIDEl = YAHOO.util.Dom.get('create_alert_action_id');
		if (!oActionIDEl){
			oActionIDEl = document.createElement('input');
			oActionIDEl.id = 'create_alert_action_id';
			oActionIDEl.setAttribute('type', 'hidden');
			oActionIDEl.setAttribute('name', 'action_id');
			oActionIDEl.setAttribute('value', action_id);
			oFormEl.appendChild(oActionIDEl);
		}
		
		var oTimeUnitEl = YAHOO.util.Dom.get('create_alert_time_unit');
		if (!oTimeUnitEl){
			oTimeUnitEl = document.createElement('input');
			oTimeUnitEl.id = 'create_alert_time_unit';
			oTimeUnitEl.setAttribute('type', 'hidden');
			oTimeUnitEl.setAttribute('name', 'time_unit');
			oTimeUnitEl.setAttribute('value', 6);
			oFormEl.appendChild(oTimeUnitEl);
		}
		
		var oCountEl = YAHOO.util.Dom.get('create_alert_count');
		if (!oCountEl){
			oCountEl = document.createElement('input');
			oCountEl.id = 'open_ticket_count';
			oCountEl.setAttribute('type', 'hidden');
			oCountEl.setAttribute('name', 'count');
			oCountEl.setAttribute('value', 6);
			oFormEl.appendChild(oCountEl);
		}
	}
	else {
		// update with the given qid
		YAHOO.util.Dom.get('create_alert_input_qid').value = p_iQid;
	}
	
	YAHOO.ELSA.createAlertDialog.show();
	YAHOO.ELSA.createAlertDialog.bringToTop();
}

YAHOO.ELSA.getPcap = function(p_sType, p_aArgs, p_oRecord){
	logger.log('p_oRecord', p_oRecord);
	
	if (!p_oRecord){
		YAHOO.ELSA.Error('Need a record selected to get pcap for.');
		return;
	}
	
	var oData = {};
	for (var i in p_oRecord.getData()['_fields']){
		oData[ p_oRecord.getData()['_fields'][i].field ] =  p_oRecord.getData()['_fields'][i].value;
	}
	var oIps = {};
	var aQuery = [];
	
	//if (defined(oData.proto) && oData.proto.toLowerCase() != 'tcp'){
	//	YAHOO.ELSA.Error('Only TCP is supported for pcap retrieval.');
	//}
	
	var aQueryParams = [ 'srcip', 'dstip', 'srcport', 'dstport' ];
	for (var i in aQueryParams){
		var sParam = aQueryParams[i];
		if (defined(oData[sParam])){
			aQuery.push(sParam + '=' + oData[sParam]);
		}
	}
	var sQuery = aQuery.join('&');
	
	// tack on the start/end +/- one minute
	var oStart = new Date( p_oRecord.getData().timestamp );
	oStart.setMinutes( p_oRecord.getData().timestamp.getMinutes() - 2 );
	var oEnd = new Date( p_oRecord.getData().timestamp );
	oEnd.setMinutes( p_oRecord.getData().timestamp.getMinutes() + 1 );
	sQuery += '&start=' + getISODateTime(oStart) + '&end=' + getISODateTime(oEnd);
	
	var oPcapWindow = window.open(YAHOO.ELSA.pcapUrl + '/?' + sQuery);
}


YAHOO.ELSA.old_getPcap = function(p_sType, p_aArgs, p_oRecord){
	logger.log('p_oRecord', p_oRecord);
	
	if (!p_oRecord){
		YAHOO.ELSA.Error('Need a record selected to get pcap for.');
		return;
	}
	
	var oData = {};
	for (var i in p_oRecord.getData()['_fields']){
		oData[ p_oRecord.getData()['_fields'][i].field ] =  p_oRecord.getData()['_fields'][i].value;
	}
	var oIps = {};
	var sQuery = 'q=';
	
	if (defined(oData.proto) && defined(oData.srcip) && defined(oData.dstip) && defined(oData.srcport) && defined(oData.dstport)){
		sQuery = oData.proto + ' ' + oData.srcip + ':' + oData.srcport + ' ' + oData.dstip + ':' + oData.dstport;
	}
	else if (defined(oData.srcip) && defined(oData.dstip)){
		sQuery = oData.srcip + ' ' + oData.dstip;
	}
	else if (defined(oData.ip)){
		sQuery = oData.ip;
	}
	else {
		// attempt to find an ip in the msg
		var re = new RegExp(/[\D](\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[\D]/);
		var aMatches = oData.msg.match(re);
		if (aMatches.length > 0){
			sQuery = aMatches[1];
		}
		else {
			YAHOO.ELSA.Error('No IP found in message');
			return;
		}
	}
	
	// tack on the start/end +/- one minute
	var oStart = new Date( p_oRecord.getData().timestamp );
	oStart.setMinutes( p_oRecord.getData().timestamp.getMinutes() - 1 );
	var oEnd = new Date( p_oRecord.getData().timestamp );
	oEnd.setMinutes( p_oRecord.getData().timestamp.getMinutes() + 1 );
	sQuery += '&start=' + getISODateTime(oStart) + '&end=' + getISODateTime(oEnd) + '&submit=1';
	
	// is the current view dev?
	var sView = '';
	if (YAHOO.ELSA.viewMode == 'dev'){
		sView = 'view=dev&';
	}
	var oPcapWindow = window.open('pcap?' + sView + 'q=' + sQuery);
	logger.log(oPcapWindow);
}

YAHOO.ELSA.getInfo = function(p_oEvent, p_oRecord){
	var oRecord = p_oRecord;
	logger.log('p_oRecord', oRecord);
	
	var oData = {};
	for (var i in oRecord.getData()['_fields']){
		oData[ oRecord.getData()['_fields'][i].field ] =  oRecord.getData()['_fields'][i].value;
	}
	logger.log('oData:', oData);
	
	var callback = {
		success: function(p_oResponse){
			var oData = YAHOO.lang.JSON.parse(p_oResponse.responseText);
			logger.log('response oData: ', oData);
			if (typeof oData.error != 'undefined'){
				YAHOO.ELSA.Error('JSON error parsing response: ' + oData.error);
				return;
			}
			YAHOO.ELSA.showLogInfo(oData, oRecord);
		},
		failure: function(oResponse){
			YAHOO.ELSA.Error('Error getting pcap.');
		}
	};
	
	var sData = 'q=' + Base64.encode(YAHOO.lang.JSON.stringify(oData));
	logger.log('sData', sData);
	
	var oConn = YAHOO.util.Connect.asyncRequest('POST', 'Query/get_log_info', callback, sData);
}

YAHOO.ELSA.showLogInfo = function(p_oData, p_oRecord){
	if (!YAHOO.ELSA.logInfoDialog){
		var handleSubmit = function(){
			this.submit();
		};
		var handleCancel = function(){
			this.hide();
		};
		var handleSuccess = function(p_oResponse){
			var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
			if (response['error']){
				YAHOO.ELSA.Error(response['error']);
			}
			else {
				YAHOO.ELSA.getQuerySchedule();
				logger.log('successful submission');
			}
		};
		var oPanel = new YAHOO.ELSA.Panel('log_info', {
			underlay: 'none',
			buttons : [ { text:"Close", handler:handleCancel } ],
			fixedcenter: true
		});
		YAHOO.ELSA.logInfoDialog = oPanel.panel;
		
		YAHOO.ELSA.logInfoDialog.callback = {
			success: handleSuccess,
			failure: YAHOO.ELSA.Error
		};
		YAHOO.ELSA.logInfoDialog.validate = function(){
			return true;
		}
	}
	
	YAHOO.ELSA.logInfoDialog.setHeader('Log Info');
	YAHOO.ELSA.logInfoDialog.setBody('');
	// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
	YAHOO.ELSA.logInfoDialog.render();
	
	var oTable = document.createElement('table');
	var oTbody = document.createElement('tbody');
	oTable.appendChild(oTbody);
	
	var oTr = document.createElement('tr');
	oTbody.appendChild(oTr);
	
	var oTd = document.createElement('td');
	oTd.innerHTML = 'Summary';
	oTr.appendChild(oTd);
	
	oTr = document.createElement('tr');
	oTbody.appendChild(oTr);
	
	oTd = document.createElement('td');
	oTd.innerHTML = p_oData.summary;
	oTr.appendChild(oTd);
	
	oTr = document.createElement('tr');
	oTbody.appendChild(oTr);
	
	oTd = document.createElement('td');
	oTd.innerHTML = 'Links';
	oTr.appendChild(oTd);
	
	oTr = document.createElement('tr');
	oTbody.appendChild(oTr);
	
	oTd = document.createElement('td');
	for (var i in p_oData.urls){
		var oA = document.createElement('a');
		oA.href = p_oData.urls[i];
		oA.innerHTML = p_oData.urls[i];
		oA.target = '_new';
		oTd.appendChild(oA);
		oTd.appendChild(document.createElement('br'));
	}
	oTr.appendChild(oTd);
	
	oTr = document.createElement('tr');
	oTbody.appendChild(oTr);
	
	oTd = document.createElement('td');
	oTd.innerHTML = 'Plugins';
	oTr.appendChild(oTd);
	
	oTr = document.createElement('tr');
	oTbody.appendChild(oTr);
	
	oTd = document.createElement('td');
	var oDiv = document.createElement('div');
	oDiv.id = 'container_log_info_plugin_select_button';
	oTd.appendChild(oDiv);
	oTr.appendChild(oDiv);
	
	YAHOO.ELSA.logInfoDialog.body.appendChild(oTable);

	//	Create an array of YAHOO.widget.MenuItem configuration properties
	var aPluginMenuSources = [ ];
	for (var i in p_oData.plugins){
		var sPluginName = p_oData.plugins[i];
		var onClick = function(){
			
		}
		aPluginMenuSources.push({
			text: sPluginName,
			onclick: { fn: YAHOO.ELSA[sPluginName], obj:p_oRecord }
		});
	}
	
	var oPluginMenuButtonCfg = {
		id: 'log_info_plugin_select_button',
		type: 'menu',
		label: 'Plugin',
		name: 'log_info_plugin_select_button',
		menu: aPluginMenuSources,
		container: oDiv.id
	};
	
	var oMenuButton = new YAHOO.widget.Button(oPluginMenuButtonCfg);
	
	YAHOO.ELSA.logInfoDialog.show();
	YAHOO.ELSA.logInfoDialog.bringToTop();
}

YAHOO.ELSA.sendToSIRT = function(p_sType, p_aArgs, p_oRecord){
	logger.log('p_oRecord', p_oRecord);
	
	if (!p_oRecord){
		YAHOO.ELSA.Error('Need a record.');
		return;
	}
	var callback = {
		success: function(oResponse){
			oSelf = oResponse.argument[0];
			if (oResponse.responseText){
				var oReturn = YAHOO.lang.JSON.parse(oResponse.responseText);
				if (typeof oReturn === 'object'){
					logger.log('attached ok');
					YAHOO.ELSA.logInfoDialog.hide();
				}
				else {
					logger.log(oReturn);
				}
			}
			else {
				logger.log(oReturn);
			}
		},
		failure: function(oResponse){
			return [ false, ''];
		},
		argument: [this]
	};
	var sPayload = YAHOO.lang.JSON.stringify(p_oRecord.getData());
	sPayload.replace(/;/, '', 'g');
	logger.log('sPayload: ' + sPayload);
	var oConn = YAHOO.util.Connect.asyncRequest('POST', YAHOO.ELSA.SIRTUrl, callback, 'data=' + Base64.encode(sPayload));
}

YAHOO.ELSA.openTicket = function(p_sType, p_aArgs, p_iQid){
	if (!YAHOO.ELSA.openTicketDialog){
		var action_id = 0;
		for (var i in YAHOO.ELSA.formParams.schedule_actions){
			if (YAHOO.ELSA.formParams.schedule_actions[i].action === 'Open Ticket'){
				action_id = YAHOO.ELSA.formParams.schedule_actions[i].action_id;
				break;
			}
		}
		
		var handleSubmit = function(){
			this.submit();
		};
		var handleCancel = function(){
			this.hide();
		};
		var handleSuccess = function(p_oResponse){
			var response = YAHOO.lang.JSON.parse(p_oResponse.responseText);
			if (response['error']){
				YAHOO.ELSA.Error(response['error']);
			}
			else {
				YAHOO.ELSA.getQuerySchedule();
				logger.log('successful submission');
			}
		};
		var oPanel = new YAHOO.ELSA.Panel('open_ticket', {
			buttons : [ { text:"Submit", handler:handleSubmit, isDefault:true },
				{ text:"Cancel", handler:handleCancel } ]
		});
		YAHOO.ELSA.openTicketDialog = oPanel.panel;
		YAHOO.ELSA.openTicketDialog.callback = {
			success: handleSuccess,
			failure: YAHOO.ELSA.Error
		};
		YAHOO.ELSA.openTicketDialog.validate = function(){
			if (!this.getData().threshold_count || !parseInt(this.getData().threshold_count)){
				YAHOO.ELSA.Error('Need a valid integer as an interval');
				return false;
			}
			if (!this.getData().threshold_time_unit || !parseInt(this.getData().threshold_time_unit)){
				YAHOO.ELSA.Error('Please select a time unit');
				return false;
			}
			if (!this.getData().assignment){
				YAHOO.ELSA.Error('Please enter a valid assignment group.');
				return false;
			}
			if (!this.getData().priority){
				YAHOO.ELSA.Error('Please enter a valid priority.');
				return false;
			}
			return true;
		}
	
		//	"click" event handler for each item in the Button's menu
		var onIntervalMenuItemClick = function(p_sType, p_aArgs, p_oItem){
			var oIntervalButton = YAHOO.widget.Button.getButton('open_ticket_interval_select_button');
			var sText = p_oItem.cfg.getProperty("text");
			// Set the label of the button to be our selection
			oIntervalButton.set('label', sText);
			var oInputEl = YAHOO.util.Dom.get('open_ticket_input_interval_unit');
			if (oInputEl){
				oInputEl.setAttribute('value', p_oItem.value);
			}
			else {
				oInputEl = document.createElement('input');
				oInputEl.id = 'open_ticket_input_interval_unit';
				oInputEl.setAttribute('type', 'hidden');
				oInputEl.setAttribute('name', 'threshold_time_unit');
				oInputEl.setAttribute('value', p_oItem.value);
				oFormEl.appendChild(oInputEl);
			}
		}
		
		//	Create an array of YAHOO.widget.MenuItem configuration properties
		var aIntervalMenuSources = [ 
			{text:'Minutes', value:'6', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Hours', value:'5', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Days', value:'4', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Weeks', value:'3', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Months', value:'2', onclick: { fn: onIntervalMenuItemClick }},
			{text:'Years', value:'1', onclick: { fn: onIntervalMenuItemClick }}
		];
		
		var oIntervalMenuButtonCfg = {
			id: 'open_ticket_interval_select_button',
			type: 'menu',
			label: 'Time Unit',
			name: 'open_ticket_interval_select_button',
			menu: aIntervalMenuSources
		};
		
		var onPriorityMenuItemClick = function(p_sType, p_aArgs, p_oItem){
			var sText = p_oItem.cfg.getProperty("text");
			// Set the label of the button to be our selection
			var oPriorityButton = YAHOO.widget.Button.getButton('open_ticket_priority_button');
			oPriorityButton.set('label', sText);
			var oInputEl = YAHOO.util.Dom.get('open_ticket_priority_input_action');
			if (oInputEl){
				oInputEl.setAttribute('value', p_oItem.value);
			}
			else {
				var oInputEl = document.createElement('input');
				oInputEl.id = 'open_ticket_priority_input_action';
				oInputEl.setAttribute('type', 'hidden');
				oInputEl.setAttribute('name', 'priority');
				oInputEl.setAttribute('value', p_oItem.value);
				oFormEl.appendChild(oInputEl);
			}
		}
		
		var aPriorityMenuItems = [];
		for (var i in YAHOO.ELSA.formParams.priority_codes){
			aPriorityMenuItems.push({
				text: YAHOO.ELSA.formParams.priority_codes[i],
				value: YAHOO.ELSA.formParams.priority_codes[i],
				onclick: { fn: onPriorityMenuItemClick }
			});
		}
		
		var oPriorityMenuButtonCfg = {
			id: 'open_ticket_priority_button',
			name: 'open_ticket_priority_button',
			type: 'menu',
			label: 'Priority',
			menu: aPriorityMenuItems
		};
		
		var oFormGridCfg = {
			form_attrs:{
				action: 'Query/schedule_query',
				method: 'POST',
				id: 'open_ticket_form'
			},
			grid: [
				[ {type:'text', args:'Create no more than one ticket every '}, {type:'input', args:{id:'open_ticket_input_interval_count', name:'threshold_count', size:2}}, {type:'widget', className:'Button', args:oIntervalMenuButtonCfg} ],
				//[ {type:'text', args:'Assignment group'}, { type:'input', args:{id:'open_ticket_assignment', name:'assignment', size:20}/*, callback:assignmentCallback*/} ],
				[ {type:'text', args:'Priority'},  {type:'widget', className:'Button', args:oPriorityMenuButtonCfg} ],
				[ {type:'input', args:{type:'hidden', id:'open_ticket_input_qid', name:'qid', value:p_iQid}} ]
			]
		};
		YAHOO.ELSA.openTicketDialog.setHeader('Open Ticket');
		YAHOO.ELSA.openTicketDialog.setBody('');
		// We need to do the initial render to auto-generate the form so we can hand that object to YAHOO.ELSA.Form
		YAHOO.ELSA.openTicketDialog.render();
		
		// Now build a new form using the element auto-generated by widget.Dialog
		var oForm = new YAHOO.ELSA.Form(YAHOO.ELSA.openTicketDialog.form, oFormGridCfg);
		// Static hidden form values
		var oFormEl = YAHOO.util.Dom.get('open_ticket_form');
		var oDaysEl = YAHOO.util.Dom.get('open_ticket_input_days');
		if (!oDaysEl){
			oDaysEl = document.createElement('input');
			oDaysEl.id = 'open_ticket_input_days';
			oDaysEl.setAttribute('type', 'hidden');
			oDaysEl.setAttribute('name', 'days');
			oDaysEl.setAttribute('value', 0);
			oFormEl.appendChild(oDaysEl);
		}
		
		var oActionIDEl = YAHOO.util.Dom.get('open_ticket_action_id');
		if (!oActionIDEl){
			oActionIDEl = document.createElement('input');
			oActionIDEl.id = 'open_ticket_action_id';
			oActionIDEl.setAttribute('type', 'hidden');
			oActionIDEl.setAttribute('name', 'action_id');
			oActionIDEl.setAttribute('value', action_id);
			oFormEl.appendChild(oActionIDEl);
		}
		
		var oTimeUnitEl = YAHOO.util.Dom.get('open_ticket_time_unit');
		if (!oTimeUnitEl){
			oTimeUnitEl = document.createElement('input');
			oTimeUnitEl.id = 'open_ticket_time_unit';
			oTimeUnitEl.setAttribute('type', 'hidden');
			oTimeUnitEl.setAttribute('name', 'time_unit');
			oTimeUnitEl.setAttribute('value', 6);
			oFormEl.appendChild(oTimeUnitEl);
		}
		
		var oCountEl = YAHOO.util.Dom.get('open_ticket_count');
		if (!oCountEl){
			oCountEl = document.createElement('input');
			oCountEl.id = 'open_ticket_count';
			oCountEl.setAttribute('type', 'hidden');
			oCountEl.setAttribute('name', 'count');
			oCountEl.setAttribute('value', 6);
			oFormEl.appendChild(oCountEl);
		}
	}
	else {
		// update with the given qid
		YAHOO.util.Dom.get('open_ticket_input_qid').value = p_iQid;
	}
	
	YAHOO.ELSA.openTicketDialog.show();
	YAHOO.ELSA.openTicketDialog.bringToTop();
}

YAHOO.ELSA.ip2long = function(ip) {
    var ips = ip.split('.');
    var iplong = 0;
    with (Math) {
        iplong = parseInt(ips[0])*pow(256,3)
        +parseInt(ip[1])*pow(256,2)
        +parseInt(ips[2])*pow(256,1)
        +parseInt(ips[3])*pow(256,0);
    }
    return iplong;
}

YAHOO.ELSA.send = function(p_sPlugin, p_sUrl, p_oData){
	logger.log('sendResults');
	if (!p_sUrl){
		throw new Error('No URL given to send results to!');
	}
	
	var oForm = document.createElement('form');
	YAHOO.util.Dom.addClass(oForm, 'hiddenElement');
	oForm.setAttribute('method', 'POST');
	oForm.setAttribute('action', p_sUrl);
	oForm.setAttribute('target', '_blank');
	
	
	var oPluginInput = document.createElement('input');
	oPluginInput.setAttribute('name', 'plugin');
	oPluginInput.setAttribute('value', p_sPlugin);
	oForm.appendChild(oPluginInput);
	
	var oDataInput = document.createElement('input');
	oDataInput.setAttribute('name', 'data');
	oDataInput.setAttribute('type', 'hidden');
	oDataInput.setAttribute('maxlength', 2147483647);
	oDataInput.setAttribute('value', encodeURIComponent(YAHOO.lang.JSON.stringify(p_oData)));
	oForm.appendChild(oDataInput);
	
	
	document.body.appendChild(oForm);
	logger.log('Sending results: ', p_oData);
	oForm.submit();
	
}

YAHOO.ELSA.Panel = function(p_sName, p_oArgs){
	this.name = p_sName;
	if (YAHOO.ELSA.panels[p_sName]){
		logger.log('YAHOO.ELSA.panels[p_sName]', YAHOO.ELSA.panels[p_sName]);
		YAHOO.ELSA.panels[p_sName].panel.setHeader('');
		YAHOO.ELSA.panels[p_sName].panel.setBody('');
		return YAHOO.ELSA.panels[p_sName];
	}
	
	var elRootDiv = document.getElementById('panel_root');
	var elNewDiv = document.createElement('div');
	elNewDiv.id = 'panel_' + p_sName;
	elRootDiv.appendChild(elNewDiv);
	this.divId = elNewDiv.id;
	
	var oPanelCfg = {
		fixedcenter: false,
		close: true,
		draggable: true,
		dragOnly: true,
		visible: false,
		constraintoviewport: true
	};
	// Override with given args
	if (p_oArgs){
		for (var key in p_oArgs){
			oPanelCfg[key] = p_oArgs[key];
		}
	}
	
	if (oPanelCfg.buttons){
		this.panel = new YAHOO.widget.Dialog(elNewDiv.id, oPanelCfg);
	}
	else {
		this.panel = new YAHOO.widget.Panel(elNewDiv.id, oPanelCfg);
	}
	
	this.panel.setBody(''); //init to empty
	this.panel.render();
	
	YAHOO.ELSA.panels[p_sName] = this; // register for possible re-use later
	YAHOO.ELSA.overlayManager.register(this.panel);
	return this;
}

YAHOO.ELSA.Panel.Confirmation = function(p_sName, p_callback, p_oCallbackArgs, p_sMessage){ 
	var oPanel = new YAHOO.ELSA.Panel(p_sName, 
		{
			buttons: [ 
				{ 
					text:"Submit", 
					handler: {
						fn: p_callback, 
						obj: p_oCallbackArgs
					}
				},
				{ text:"Cancel", handler:function(){ this.hide(); }, isDefault:true } 
			]
		}
	);
	this.panel = oPanel.panel;
	this.panel.setHeader('Confirm');
	this.panel.setBody(p_sMessage);
	this.panel.render();
	this.panel.show();
}

YAHOO.ELSA.Warn = function(p_sMessage){
	logger.log('WARNING: ' + p_sMessage);
}
