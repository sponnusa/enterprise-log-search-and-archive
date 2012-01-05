
var logger; //pretty global, but everybody needs it

YAHOO.ELSA.main = function () {
	// Set viewMode for dev/prod
	var oRegExp = new RegExp('\\Wview=(\\w+)');
	var oMatches = oRegExp.exec(location.search);
	if (oMatches){
		YAHOO.ELSA.viewMode = oMatches[1];
	}
	
	YAHOO.ELSA.initLogger();
	
	YAHOO.ELSA.currentQuery = new YAHOO.ELSA.Query();
	
	var submitQuery = function(){
		// apply the start/stop times
		try {
			var oStartTime, oEndTime;
			if (YAHOO.util.Dom.get('start_time').value){
				oStartTime = getDateFromISO(YAHOO.util.Dom.get('start_time').value)/1000;
				if (!oStartTime){
					YAHOO.ELSA.Error('Invalid start time');
					return;
				}
				else {
					YAHOO.ELSA.currentQuery.addMeta('start', oStartTime);
				}
			}
			else {
				YAHOO.ELSA.currentQuery.delMeta('start');
			}
			if (YAHOO.util.Dom.get('end_time').value){
				oEndTime = getDateFromISO(YAHOO.util.Dom.get('end_time').value)/1000;
				if (!oEndTime){
					YAHOO.ELSA.Error('Invalid end time');
					return;
				}
				else {
					YAHOO.ELSA.currentQuery.addMeta('end', oEndTime);
				}
			}
			else {
				YAHOO.ELSA.currentQuery.delMeta('end');
			}
			if (oStartTime > oEndTime){
				YAHOO.ELSA.Error('Start time greater than end time');
				return;
			}
			logger.log('submitting query: ', YAHOO.ELSA.currentQuery);
						
			var oResults = new YAHOO.ELSA.Results.Tabbed.Live(YAHOO.ELSA.tabView, YAHOO.ELSA.currentQuery);
			logger.log('got query results:', oResults);
			YAHOO.ELSA.currentQuery.resetTerms();
		} catch(e) { YAHOO.ELSA.Error(e); }
	}	
	
	var drawQueryForm = function(){		
		var oDialog=null;
	
		YAHOO.ELSA.formParams = formParams;
		YAHOO.ELSA.formParams.classIdMap = formParams['classes'];
		YAHOO.ELSA.formParams.classIdMap['0'] = 'ALL';
		
		var oSubmitButtonConfig = { 
			type: "button", 
			label: "Submit Query", 
			id: "query_submit"
		};
		
		var onTermMetaSelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('adding meta', p_oItem);
			var id = this.element.id;
			logger.log('id:' + this.element.id);
			var op = '=';
			if (p_oItem.className){
				YAHOO.ELSA.currentQuery.addTerm('class', p_oItem.className, op);
			}
			if (p_oItem.program){
				YAHOO.ELSA.currentQuery.addTerm('program', p_oItem.program, op);
			}
			
		}
		
		var onTermSelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('this', this);
			logger.log('p_aArgs', p_aArgs);
			logger.log('p_oItem', p_oItem);
			//logger.log('oMenu', oMenu);
			var id = this.element.id;
			logger.log('id:' + this.element.id);
			var oPanel = new YAHOO.ELSA.Panel(this.element.id, {context:[this.element.id, 'tl', 'tr']});
			// find field type to determine if we can do a range comparison
			var sType = 'string';
			for (var i in YAHOO.ELSA.formParams.fields){
				//logger.log(YAHOO.ELSA.formParams.fields[i]);
				//logger.log(YAHOO.ELSA.formParams.fields[i].field_type + ' ' + this.element.id);
				if (YAHOO.ELSA.formParams.fields[i].fqdn_field === this.element.id){
					sType = YAHOO.ELSA.formParams.fields[i].field_type;
					break;
				}
			}
			if (sType === 'int'){
				var elOperator = document.createElement('select');
				elOperator.id = 'add_term_op_' + this.element.id;
				var aOps = ['=', '!=', '>=', '<='];
				for (var i in aOps){
					var elOption = document.createElement('option');
					elOption.value = aOps[i];
					elOption.innerHTML = aOps[i];
					elOperator.appendChild(elOption);
				}
				oPanel.panel.body.appendChild(elOperator);
			}
			var elInput = document.createElement('input');
			elInput.type = 'text';
			elInput.id = 'add_term_' + this.element.id;
			oPanel.panel.setHeader(p_oItem.fqdn_field);
			oPanel.panel.body.appendChild(elInput);
			var fnButtonSubmit = function(oEvent){
				// Make sure we don't submit the form
				YAHOO.util.Event.stopEvent(oEvent);
				var tgt=(oEvent.target ? oEvent.target : 
					(oEvent.srcElement ? oEvent.srcElement : null)); 
				try{
					tgt.blur();
				}
				catch(e){}
				var op = '=';
				if (YAHOO.util.Dom.get('add_term_op_' + id)){
					op = YAHOO.util.Dom.get('add_term_op_' + id).value;
				}
				if (YAHOO.ELSA.currentQuery.addTerm(p_oItem.fqdn_field, elInput.value, op, elInput)){
					oPanel.panel.hide();
				}
			};
			var oSubmitButtonCfg = {
				container: oPanel.panel.body,
				id: 'term_menu_submit_button',
				type: 'button',
				label: 'Add',
				name: 'term_menu_submit_button',
				onclick: { 	
					fn: fnButtonSubmit,
					scope: YAHOO.ELSA,
					correctScope: false
				}
			};
			var oSubmitButton = new YAHOO.widget.Button(oSubmitButtonCfg);
			oPanel.panel.show();
			elInput.focus();
			var enterKeyListener = new YAHOO.util.KeyListener(
					elInput,
					{ keys: 13 },
					{ 	
						fn: function(eName, p_aArgs){
							var oEvent = p_aArgs[1];
							fnButtonSubmit(oEvent);
						},
						scope: YAHOO.ELSA,
						correctScope: false
					}
			);
			enterKeyListener.enable();
		}
		
		// Build term selection menu
		var oFields = {
			'none': {
				text: 'Unclassified',
				submenu: { id:'none', itemdata: [
					{
						text: 'Unclassified',
						id: 'none',
						onclick: { 
							fn: onTermMetaSelectionClick,
							obj: { className:'none', id:'none' }
						}
					}
				] }
			}
		};
		for (var i in YAHOO.ELSA.formParams.fields){
			var sClass = YAHOO.ELSA.formParams.fields[i]['class'];
			var sField = YAHOO.ELSA.formParams.fields[i]['value'];
			if (!sClass){
				continue;
			}
			if (!oFields[sClass]){
				// find class id
				var iClassId;
				for (var j in YAHOO.ELSA.formParams.classes){
					if (sClass === YAHOO.ELSA.formParams.classes[j]){
						iClassId = j;
						break;
					}
				}
				
				oFields[sClass] = {
					text: sClass,
					submenu: { id:sClass, itemdata: [
						{
							text: 'Class ' + sClass,
							id: sClass,
							onclick: { 
								fn: onTermMetaSelectionClick,
								obj: { className:sClass, id:sClass }
							}
						}
					] }
				};
			}
			oFields[sClass]['submenu']['itemdata'].push({
				text: 'Field ' + sField,
				id: sClass + '.' + sField,
				onclick: { fn:onTermSelectionClick, obj:{ fqdn_field:sClass + '.' + sField, id:sClass + '_' + sField } }
			});
		}
		
		var aTermMenuItems = [];
		for (var sClass in oFields){
			aTermMenuItems.push(oFields[sClass]);
		}
		logger.log('aTermMenuItems', aTermMenuItems);
		
		var oTermMenuButtonCfg = {
			id: 'term_menu_select_button',
			type: 'menu',
			label: YAHOO.ELSA.Labels.noTerm,
			name: 'term_menu_select_button',
			menu: aTermMenuItems
		};
		
		// Groupby menu
	
		//	"click" event handler for each item in the Button's menu
		var onGroupBySelectionClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('p_oItem:', p_oItem);
			var sText = p_oItem.fqdn_field;
			var aClassVal = sText.split(/\./);
			// Set the label of the button to be our selection
			var oButton = YAHOO.widget.Button.getButton('groupby_menu_select_button');
			oButton.set('label', sText);
			logger.log('oButton:', oButton);
			
			// reset old values
			YAHOO.ELSA.currentQuery.delMeta('groupby');
			YAHOO.ELSA.currentQuery.delMeta('groups_only');
			YAHOO.ELSA.currentQuery.delMeta('class');
			YAHOO.ELSA.currentQuery.delMeta('limit');
			
			if (aClassVal[0] == 'any'){
				//any class, always an INT field
				YAHOO.ELSA.currentQuery.addMeta('class', 'any');
				YAHOO.ELSA.currentQuery.addMeta('groupby', [aClassVal[1]]);
				YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
			}
			else if (aClassVal[0] != YAHOO.ELSA.Labels.noGroupBy){ //clears
				// Find type to determine if we can do this remotely or if it's a client-side group
				var sFieldType = 'string';
				for (var i in YAHOO.ELSA.formParams.fields){
					if (YAHOO.ELSA.formParams.fields[i].fqdn_field === sText){
						sFieldType = YAHOO.ELSA.formParams.fields[i].field_type;
						break;
					}
				}
				
				YAHOO.ELSA.currentQuery.addMeta('class', aClassVal[0]);
				YAHOO.ELSA.currentQuery.addMeta('groupby', [aClassVal[1]]);
				YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
			}
		}
		
		var aUnclassedFields = ['Host', 'Class', 'Program', 'Day', 'Hour', 'Minute', 'Timestamp', 'Node'];
		var aUnclassedItems = [];
		for (var i in aUnclassedFields){
			var sPrettyClass = aUnclassedFields[i];
			var sClass = sPrettyClass.toLowerCase();
			aUnclassedItems.push({
				text: sPrettyClass,
				id: 'groupby_any.' + sClass,
				onclick: { 
					fn: onGroupBySelectionClick, 
					obj: { 
						fqdn_field: 'any' + '.' + sClass, 
						id: 'any' + '_' + sClass 
					} 
				}
			});
		}
		
		// Build term selection menu
		var aGroupByMenuItems = [
			{
				text: YAHOO.ELSA.Labels.noGroupBy,
				onclick: { fn:onGroupBySelectionClick, obj:{ fqdn_field:YAHOO.ELSA.Labels.noGroupBy } }
			},
			{
				text: 'All Classes',
				onclick: { fn:onGroupBySelectionClick, obj:{ fqdn_field:'Any', id:'groupby_Any' } },
				submenu: {
					id: 'groupby_Any',
					itemdata: aUnclassedItems
				}
			}
		];
		var oGroupByFields = {};
		for (var i in YAHOO.ELSA.formParams.fields){
			var sClass = YAHOO.ELSA.formParams.fields[i]['class'];
			var sField = YAHOO.ELSA.formParams.fields[i]['value'];
			if (!sClass){
				continue;
			}
			if (!oGroupByFields[sClass]){
				oGroupByFields[sClass] = {
					text: sClass,
					submenu: { id:'groupby_' + sClass, itemdata: [] }
				};
			}
			oGroupByFields[sClass]['submenu']['itemdata'].push({
				text: sField,
				id: 'groupby_' + sClass + '.' + sField,
				onclick: { fn:onGroupBySelectionClick, obj:{ fqdn_field:sClass + '.' + sField, id:sClass + '_' + sField } }
			});
		}
		for (var i in oGroupByFields){
			aGroupByMenuItems.push(oGroupByFields[i]);
		}
		logger.log('aGroupByMenuItems', aGroupByMenuItems);
		
		var oGroupByMenuButtonCfg = {
			id: 'groupby_menu_select_button',
			type: 'menu',
			label: YAHOO.ELSA.Labels.defaultGroupBy,
			name: 'groupby_menu_select_button',
			menu: aGroupByMenuItems
		};
		
		var oArchiveButtonCfg = {
			id: 'archive_button',
			type: 'checkbox',
			label: 'Index',
			name: 'archive_button',
			value: 'archive_query',
			checked: false,
			onclick: {
				fn: function(p_oEvent){
					logger.log('arguments', arguments);
					if (p_oEvent.target.innerHTML == 'Index'){
						YAHOO.ELSA.currentQuery.addMeta('archive_query', 1);
						p_oEvent.target.innerHTML = 'Archive';
					}
					else {
						YAHOO.ELSA.currentQuery.delMeta('archive_query');
						p_oEvent.target.innerHTML = 'Index';
					}
				}
			}
		}
		
		/* Draw form */
		
		var oQueryFormGridCfg = {
			form_attrs:{
				id: 'query_menu'
			}
		};
		oQueryFormGridCfg['grid'] = [
			[ 
				{type:'text', args:'Query'},
				{type:'input', args:{id:'q', size:80} },
				{type:'widget', className:'Button', args:oSubmitButtonConfig},
				{type:'element', element:'a', args:{href:'http://code.google.com/p/enterprise-log-search-and-archive/wiki/Documentation#Queries', innerHTML:'Help', target:'_new'}}
			]
		];
		
		var oFormGridCfg = {
			form_attrs:{
				id: 'query_menu'
			}
		};
		
		var oStartDate = new Date((formParams.display_start_int) * 1000);
				
		oFormGridCfg['grid'] = [
			[ 
				{type:'text', args:'From'}, 
				{type:'input', args:{id:'start_time', size:15, value:getISODateTime(oStartDate)}}, 
				{type:'text', args:'To'},
				{type:'input', args:{id:'end_time', size:15}},
				{type:'widget', className:'Button', args:oTermMenuButtonCfg},
				{type:'widget', className:'Button', args:oGroupByMenuButtonCfg},
				{type:'widget', className:'Button', args:oArchiveButtonCfg}
			]
		];
		
		try {
			
			var oTargetForm = document.createElement('form');
			YAHOO.ELSA.queryForm = new YAHOO.ELSA.Form(oTargetForm, oQueryFormGridCfg);
			new YAHOO.ELSA.Form(oTargetForm, oFormGridCfg);
			
			YAHOO.util.Dom.get('query_form').appendChild(oTargetForm);
			
			// Tack on the tooltip for earliest start date
			var oStartToolTip = new YAHOO.widget.Tooltip('start_time_tool_tip', { context: 'start_time', text: 'Earliest: ' + formParams.start});
			var oStartToolTip = new YAHOO.widget.Tooltip('end_time_tool_tip', { context: 'end_time', text: 'Latest: ' + formParams.end});
						
			/* Put the cursor in the main search field */
			if (YAHOO.util.Dom.get('q')){
				YAHOO.util.Dom.get('q').focus();	
			}
			else {
				YAHOO.ELSA.Error('Unable to find query input field');
				return;	
			}
			
		}
		catch (e){
			YAHOO.ELSA.Error('Error drawing query grid:' + e.toString());
			return;	
		}
				
		/* Have the enter key submit the form */
		var enterKeyListener = new YAHOO.util.KeyListener(
				document,
				{ keys: 13 },
				{ 	fn: function(eName, eObj){ var tgt=(eObj[1].target ? eObj[1].target : (eObj[1].srcElement ? eObj[1].srcElement : null)); try{tgt.blur();}catch(e){} submitQuery();},
					scope: YAHOO.ELSA,
					correctScope: false
				}
		);
		enterKeyListener.enable();
	}

	var drawMenuBar = function(){
		
		var aItemData = [
			{
				text: 'ELSA',
				submenu: {
					id: 'queries_menu',
					itemdata: [
						{
							text: 'Query Log',
							helptext: 'Queries this user has previously run',
							onclick: {
								fn: YAHOO.ELSA.getPreviousQueries
							}
						},
						{
							text: 'Saved Results',
							helptext: 'Results this user has manually saved',
							onclick: {
								fn: YAHOO.ELSA.getSavedQueries
							}
						},
						{
							text: 'Alerts',
							helptext: 'Currently scheduled queries',
							onclick: {
								fn: YAHOO.ELSA.getQuerySchedule
							}
						},
						{
							text: 'Cancel Queries',
							helptext: 'Currently running queries',
							onclick: {
								fn: YAHOO.ELSA.getRunningArchiveQuery
							}
						}
					]
				}
			}
		];
		
		if (typeof YAHOO.ELSA.IsAdmin != 'undefined'){
			aItemData.push({
				text: 'Admin',
				submenu: {
					id: 'admin_menu',
					itemdata: [
						{
							text: 'Manage Permissions',
							helptext: 'Manage permissions for users',
							url: 'admin',
							target: '_new'
						},
						{
							text: 'Stats',
							helptext: 'Query and load statistics',
							url: 'stats',
							target: '_new'
						}
					]
				}
			});
		}
		
		var oMenuBar = new YAHOO.widget.MenuBar('menu_bar_content', {
			lazyload: false,
			itemdata: aItemData
		});
		oMenuBar.render('menu_bar');
		YAHOO.util.Dom.addClass(oMenuBar.element, "yuimenubarnav");
		// Fix z-index issues so that the menu is always on top
		var menuEl = new YAHOO.util.Element('queries_menu');
		menuEl.setStyle('z-index', 1000);
	}
	
	drawMenuBar();
	
	/* Get form params (goes all the way to a backend node) */
	drawQueryForm();
	
	/* Instantiate the tab view for our results */
	var setActiveQuery = function(p_oEvent){
		logger.log('set active query p_oEvent', p_oEvent);
		p_oTab = p_oEvent.newValue;
		
		var iTabIndex = YAHOO.ELSA.tabView.getTabIndex(p_oTab);
		if (typeof iTabIndex == 'undefined'){
			logger.log('unable to find tabindex for tab:', p_oTab);
			return;
		}
		
		// find the result that has this tabid
		var iLocalResultId = YAHOO.ELSA.getLocalResultId(p_oTab);
		var oQuery;
		if (iLocalResultId){
			try {
				logger.log('local result: ', YAHOO.ELSA.localResults[iLocalResultId]);
				if (typeof(YAHOO.ELSA.localResults[iLocalResultId].queryString) != 'undefined' &&
					typeof(YAHOO.ELSA.localResults[iLocalResultId].query) != 'undefined' &&
					typeof(YAHOO.ELSA.localResults[iLocalResultId].query.metas) != 'undefined'){
					oQuery = { 
						'query_string': YAHOO.ELSA.localResults[iLocalResultId].queryString,
						'query_meta_params': YAHOO.ELSA.localResults[iLocalResultId].query.metas
					};
				}
				else {
					logger.log('parsing ' + YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
					oQuery = YAHOO.lang.JSON.parse(YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
					//oQuery = YAHOO.ELSA.localResults[iLocalResultId].results;
				}
			}
			catch (e){
				logger.log('error getting query for results:', e);
				logger.log('results:', YAHOO.ELSA.localResults[iLocalResultId]);
				return;
			}
			YAHOO.ELSA.currentQuery.queryString = oQuery.query_string;
			logger.log('set active query: ', oQuery);
			// set the q bar
			YAHOO.util.Dom.get('q').value = oQuery.query_string;
			
			//set the groupby button
			var oGroupButton = YAHOO.widget.Button.getButton('groupby_menu_select_button');
			var oArchiveButton = YAHOO.widget.Button.getButton('archive_button');
			if (oQuery.query_meta_params){
				YAHOO.ELSA.currentQuery.metas = oQuery.query_meta_params;
				if (typeof(YAHOO.ELSA.currentQuery.metas.groupby) == 'undefined'){
					// groupby could've been set in query text instead of data struct
					if (typeof(YAHOO.ELSA.localResults[iLocalResultId].results.groupby) != 'undefined'){
						logger.log('setting groupby from results: ', YAHOO.ELSA.localResults[iLocalResultId].results.groupby);
						YAHOO.ELSA.currentQuery.metas.groupby = YAHOO.ELSA.localResults[iLocalResultId].results.groupby;
					}
				}
				logger.log('current query: ' + YAHOO.lang.JSON.stringify(YAHOO.ELSA.currentQuery));
				logger.log('type of class: ' + typeof YAHOO.ELSA.currentQuery.metas['class']);
				logger.log('current groupby:', YAHOO.ELSA.currentQuery.metas.groupby);
				if (YAHOO.ELSA.currentQuery.metas.groupby){
					if (typeof YAHOO.ELSA.currentQuery.metas['class'] != 'undefined'){
						oGroupButton.set('label', YAHOO.ELSA.currentQuery.metas['class'] + '.' + YAHOO.ELSA.currentQuery.metas.groupby);
					}
					else {
						oGroupButton.set('label', 'any.' + YAHOO.ELSA.currentQuery.metas.groupby[0]);
					}
				}
				else {
					oGroupButton.set('label', YAHOO.ELSA.Labels.defaultGroupBy);
				}
				// set times
				if (YAHOO.ELSA.currentQuery.metas.start){
					YAHOO.util.Dom.get('start_time').value = getISODateTime(new Date(YAHOO.ELSA.currentQuery.metas.start * 1000));
				}
				if (YAHOO.ELSA.currentQuery.metas.end){
					YAHOO.util.Dom.get('end_time').value = getISODateTime(new Date(YAHOO.ELSA.currentQuery.metas.end * 1000));
				}
				
				//set the archive button
				if (typeof(YAHOO.ELSA.currentQuery.metas.groupby) == 'undefined'){
					// groupby could've been set in query text instead of data struct
					if (typeof(YAHOO.ELSA.localResults[iLocalResultId].results.query_meta_params) != 'undefined' &&
						typeof(YAHOO.ELSA.localResults[iLocalResultId].results.query_meta_params.archive_query) != 'undefined'){
						YAHOO.ELSA.currentQuery.metas.archive_query = YAHOO.ELSA.localResults[iLocalResultId].results.query_meta_params.archive_query;
					}
				}
				logger.log('current query: ' + YAHOO.lang.JSON.stringify(YAHOO.ELSA.currentQuery));
				logger.log('type of class: ' + typeof YAHOO.ELSA.currentQuery.metas['class']);
				logger.log('current archive:', YAHOO.ELSA.currentQuery.metas.archive_query);
				if (YAHOO.ELSA.currentQuery.metas.archive_query){
					oArchiveButton.set('label', YAHOO.ELSA.Labels.archive);
					oArchiveButton.set('checked', true);
				}
				else {
					oArchiveButton.set('label', YAHOO.ELSA.Labels.index);
					oArchiveButton.set('checked', false);
				}
			}
			else {
				logger.log('no metas found from oQuery');
				oGroupButton.set('label', YAHOO.ELSA.Labels.defaultGroupBy);
				oArchiveButton.set('label', YAHOO.ELSA.Labels.index);
				oArchiveButton.set('checked', false);
			}
		}
		else {
			logger.log('iLocalResultId was undefined');
		}
	}
	var oTabViewDiv = YAHOO.util.Dom.get('tabView');
	YAHOO.util.Dom.addClass(oTabViewDiv, 'hiddenElement');
	YAHOO.ELSA.tabView = new YAHOO.widget.TabView(oTabViewDiv);
	YAHOO.ELSA.tabView.subscribe('activeTabChange', setActiveQuery);
	
	YAHOO.util.Event.addListener('query_submit', 'click', submitQuery);
};

