
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
		if (YAHOO.util.Dom.get('start_time').value){
			var sStartTime = getDateFromISO(YAHOO.util.Dom.get('start_time').value)/1000;
			YAHOO.ELSA.currentQuery.addMeta('start', sStartTime);
		}
		if (YAHOO.util.Dom.get('end_time').value){
			var sEndTime = getDateFromISO(YAHOO.util.Dom.get('end_time').value)/1000;
			YAHOO.ELSA.currentQuery.addMeta('end', sEndTime);
		}
		logger.log('submitting query: ', YAHOO.ELSA.currentQuery);
		try {
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
		
		
		/* Draw start/end calendars */
		var oMinDate = new Date();
		var oMaxDate = new Date();
		var oParamMin = getDateFromISO(YAHOO.ELSA.formParams['start']);
		var oParamMax = getDateFromISO(YAHOO.ELSA.formParams['end']);
		if(oParamMin){
			oMinDate.setTime(oParamMin);
		}
		if(oParamMax){
			oMaxDate.setTime(oParamMax);
		}
		logger.log('using min: ' + oMinDate + ', max: ' + oMaxDate);
		
		var onButtonClick = function(p_sId){
			var oDialog = new YAHOO.widget.Panel('cal_' + p_sId + '_container', {
				visible:false,
				context:['cal_' + p_sId + '_button', 'tl', 'bl'],
				draggable:false,
				close:true
			});
			oDialog.setHeader('Start Day');
			oDialog.setBody('<div id="cal_' + p_sId + '_div_container"></div>');
			oDialog.render(YAHOO.util.Dom.get('query_form'));
				
			var oCalendar = new YAHOO.widget.Calendar('cal_' + p_sId + '_div_container',
				{ 
					iframe: false,
					hide_blank_weeks: true,
					pagedate: oMinDate,
					mindate: oMinDate,
					maxdate: oMaxDate
				});
			oCalendar.render();
			
			oCalendar.selectEvent.subscribe(function(p_sType, p_aArgs){
				var aDate;
				try {
				if (p_aArgs){
					aDate = p_aArgs[0][0];
					// get previous time
					var re = new RegExp(/(\d{2}:\d{2}:\d{2})/);
					var aTime = re.exec(YAHOO.util.Dom.get(p_sId + '_time').value);
					var sTime = '00:00:00';
					if (aTime){
						sTime = aTime[0];
					}
					var sNewDateTime = formatDateTimeAsISO(aDate[1] + '/' + aDate[2] + '/' + aDate[0] + ' ' + sTime);
					YAHOO.util.Dom.get(p_sId + '_time').value = sNewDateTime;
					YAHOO.ELSA.currentQuery.addMeta(p_sId + '_time', sNewDateTime);
				}
				} catch (e){ logger.log(e) }
				
				oDialog.hide();
			});
			
			// Hide Calendar if we click anywhere in the document other than the calendar
		    YAHOO.util.Event.on(document, "click", function(e) {
				var el = YAHOO.util.Event.getTarget(e);
				var oDialogEl = oDialog.element;
				
				if (el != oDialogEl 
					&& !YAHOO.util.	Dom.isAncestor(oDialogEl, el) 
					&& el != YAHOO.util.Dom.get('cal_' + p_sId + '_button') 
					&& !YAHOO.util.Dom.isAncestor(YAHOO.util.Dom.get('cal_' + p_sId + '_button'), el)) {
					oDialog.hide();
				}
			});
							
			oDialog.show();
		}	
		
		var oCalStartButtonConfig = { 
			type: 'button', 
			label: 'Start', 
			id: 'cal_start_button', 
			value: 'Start',
			onclick: {
				fn: function(){ onButtonClick('start'); }
			}
		};
		
		var oCalEndButtonConfig = { 
			type: 'button', 
			label: 'End', 
			id: 'cal_end_button', 
			value: 'End',
			onclick: {
				fn: function(){ onButtonClick('end'); }
			}
		};
		
		var oSubmitButtonConfig = { 
			type: "button", 
			label: "Submit Query", 
			id: "query_submit"
		};
		
		var onTermHostClick = function(p_sType, p_aArgs, p_oItem){
			logger.log('p_aArgs', p_aArgs);
			logger.log('p_oItem', p_oItem);
			var oPanel = new YAHOO.ELSA.Panel(this.element.id, {context:[this.element.id, 'tl', 'tr']});
			var elInput = document.createElement('input');
			elInput.type = 'text';
			elInput.id = 'add_term_' + this.element.id;
			oPanel.panel.setHeader('Host');
			oPanel.panel.body.appendChild(elInput);
			oPanel.panel.show();
			elInput.focus();
			var id = this.element.id;
			var enterKeyListener = new YAHOO.util.KeyListener(
					elInput,
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
							if (YAHOO.util.Dom.get('add_term_op_' + id)){
								op = YAHOO.util.Dom.get('add_term_op_' + id).value;
							}
							if (YAHOO.ELSA.currentQuery.addTerm('host', elInput.value, op, elInput)){
								oPanel.panel.hide();
							}
						},
						scope: YAHOO.ELSA,
						correctScope: false
					}
			);
			enterKeyListener.enable();
		}
		
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
			/*var div = document.createElement('div');
			div.id = 'target_' + this.element.id;
			YAHOO.util.Dom.get(this.element.id).appendChild(div);*/
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
				var aOps = ['=', '>=', '<='];
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
			oPanel.panel.show();
			elInput.focus();
			var enterKeyListener = new YAHOO.util.KeyListener(
					elInput,
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
							if (YAHOO.util.Dom.get('add_term_op_' + id)){
								op = YAHOO.util.Dom.get('add_term_op_' + id).value;
							}
							if (YAHOO.ELSA.currentQuery.addTerm(p_oItem.fqdn_field, elInput.value, op, elInput)){
								oPanel.panel.hide();
							}
						},
						scope: YAHOO.ELSA,
						correctScope: false
					}
			);
			enterKeyListener.enable();
		}
		
		// Build term selection menu
		var oFields = {
			'hosts': {
				text: 'Host',
				onclick: { 
					fn: onTermHostClick
				}
			},
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
			label: 'Add Term',
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
			YAHOO.ELSA.currentQuery.delMeta('local_groupby');
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
				// Only int field types can be grouped remotely in Sphinx
				if (sFieldType === 'int'){
					YAHOO.ELSA.currentQuery.addMeta('groupby', [aClassVal[1]]);
					YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
				}
				else {
					YAHOO.ELSA.currentQuery.addMeta('local_groupby', [aClassVal[1]]);
					//make sure we've got as many results to process as possible
					YAHOO.ELSA.currentQuery.addMeta('limit', YAHOO.ELSA.localGroupByQueryLimit);
				}
			}
		}
		
		var aUnclassedFields = ['Host', 'Class', 'Program', 'Day', 'Hour', 'Minute', 'Timestamp'];
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
				//id: 'groupby_Any',
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
			label: YAHOO.ELSA.Labels.noGroupBy,
			name: 'groupby_menu_select_button',
			menu: aGroupByMenuItems
		};
		
		var onRadioSelect = function(p_oEvent){
			//logger.log('p_oEvent', p_oEvent);
			YAHOO.ELSA.currentQuery.setBoolean(p_oEvent.newValue.get('name'));
		}
		var radioButtonCallback = function(p_oArgs, p_oInputEl, p_oEl){
			logger.log('p_oArgs', p_oArgs);
			logger.log('p_oInputEl', p_oInputEl);
			logger.log('p_oEl', p_oEl);
			var oButtonGroup = p_oInputEl;
			var aBooleans = ['OR', 'AND', 'NOT'];
			var aButtons = [];
			for (var i in aBooleans){
				var checked = false;
				if (aBooleans[i] == p_oArgs.args.checkedButton){
					checked = true;
				}
				aButtons.push({
					name: aBooleans[i],
					label: aBooleans[i],
					value: aBooleans[i],
					id: 'query_form_boolean_' + aBooleans[i].toLowerCase(),
					checked: checked
				});
			}
			logger.log('aButtons', aButtons);
			oButtonGroup.addButtons(aButtons);
			oButtonGroup.subscribe('checkedButtonChange', onRadioSelect);
			logger.log('oButtonGroup:', oButtonGroup);
		}
		
		var oArchiveButtonCfg = {
			id: 'archive_button',
			type: 'checkbox',
			label: 'Index',
			name: 'groupby_menu_select_button',
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
		var oFormGridCfg = {
			form_attrs:{
				id: 'query_menu'
			}
		};
		
		if (false){
		oFormGridCfg['grid'] = [
			[ {type:'text', args:'Query'}, {type:'input', args:{id:'q', size:80} }, {type:'widget', className:'Button', args:oSubmitButtonConfig} ],
			[ {type:'text', args:'Start Time'}, [ {type:'input', args:{id:'start_time', size:20, value:formParams.start}}, {type:'widget', className:'Button', args:oCalStartButtonConfig } ] ],
			[ {type:'text', args:'End Time'}, [ {type:'input', args:{id:'end_time', size:20/*, value:oFormParams.end*/}}, {type:'widget', className:'Button', args:oCalEndButtonConfig } ] ],
			[ {type:'text', args:'Add Search Field'}, [ {type:'widget', className:'Button', args:oTermMenuButtonCfg}, {type:'widget', className:'ButtonGroup', args:{id:'query_form_boolean', checkedButton:'OR'}, callback:radioButtonCallback }] ],
			[ {type:'text', args:'Group By'}, {type:'widget', className:'Button', args:oGroupByMenuButtonCfg} ]
		];}
		
		oFormGridCfg['grid'] = [
			[ 
				{type:'text', args:'Query'},
				{type:'input', args:{id:'q', size:80} }, 
				{type:'widget', className:'Button', args:oSubmitButtonConfig} 
			]
		];
		
		var oFormGridCfg2 = {
			form_attrs:{
				id: 'query_menu'
			}
		};
		
		oFormGridCfg2['grid'] = [
			[ 
				{type:'text', args:'Start Time'}, 
				{type:'input', args:{id:'start_time', size:20, value:formParams.start}}, 
				{type:'widget', className:'Button', args:oCalStartButtonConfig },
				{type:'text', args:'Add Term'},  
				{type:'widget', className:'ButtonGroup', args:{id:'query_form_boolean', checkedButton:'OR'}, callback:radioButtonCallback },
				{type:'text', args:'Group By'}, 
				{type:'widget', className:'Button', args:oGroupByMenuButtonCfg}
			],
			[
				{type:'text', args:'End Time'}, 
				{type:'input', args:{id:'end_time', size:20}}, 
				{type:'widget', className:'Button', args:oCalEndButtonConfig },
				{type:'text', args:' '}, 
				{type:'widget', className:'Button', args:oTermMenuButtonCfg},
				{type:'text', args:'Use'},
				{type:'widget', className:'Button', args:oArchiveButtonCfg}
			]
		];
		
		
		try {
			
			var oTargetForm = document.createElement('form');
			YAHOO.ELSA.queryForm = new YAHOO.ELSA.Form(oTargetForm, oFormGridCfg);
			var new2 = new YAHOO.ELSA.Form(oTargetForm, oFormGridCfg2);
			
			YAHOO.util.Dom.get('query_form').appendChild(oTargetForm);
			
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
			var str;
			for (var i in e){
				str = str + i + ' ' + e[i];
			}
			YAHOO.ELSA.Error('Error drawing query grid:' + str);
			return;	
		}
				
		/* Have the enter key submit the form */
		var enterKeyListener = new YAHOO.util.KeyListener(
				//YAHOO.util.Dom.get('query_form'),
				document,
				{ keys: 13 },
				{ 	fn: function(eName, eObj){ var tgt=(eObj[1].target ? eObj[1].target : (eObj[1].srcElement ? eObj[1].srcElement : null)); try{tgt.blur();}catch(e){} submitQuery();},
					scope: YAHOO.ELSA,
					correctScope: false
				}
		);
		enterKeyListener.enable();
	}
	
	var old_drawQueryForm = function(p_oResponse){
		try{
			var oFormParams = {}, oDialog=null;
			if(p_oResponse.responseText !== undefined && p_oResponse.responseText){
				try{
					oFormParams = YAHOO.lang.JSON.parse(p_oResponse.responseText);
					if (oFormParams.error){
						YAHOO.ELSA.Error(oFormParams.error);
						return;
					}
				}catch(e){
					logger.log('Could not parse response for form parameters because of an error: '+e);
					//return false;
				}
				YAHOO.ELSA.formParams = oFormParams;
				YAHOO.ELSA.formParams.classIdMap = oFormParams['classes'];
				
				YAHOO.ELSA.formParams.classIdMap['0'] = 'ALL';
				
				
				/* Draw start/end calendars */
				var oMinDate = new Date();
				var oMaxDate = new Date();
				var oParamMin = getDateFromISO(oFormParams['start']);
				var oParamMax = getDateFromISO(oFormParams['end']);
				if(oParamMin){
					oMinDate.setTime(oParamMin);
				}
				if(oParamMax){
					oMaxDate.setTime(oParamMax);
				}
				logger.log('using min: ' + oMinDate + ', max: ' + oMaxDate);
				
				var onButtonClick = function(p_sId){
					var oDialog = new YAHOO.widget.Panel('cal_' + p_sId + '_container', {
						visible:false,
						context:['cal_' + p_sId + '_button', 'tl', 'bl'],
						draggable:false,
						close:true
					});
					oDialog.setHeader('Start Day');
					oDialog.setBody('<div id="cal_' + p_sId + '_div_container"></div>');
					oDialog.render(YAHOO.util.Dom.get('query_form'));
						
					var oCalendar = new YAHOO.widget.Calendar('cal_' + p_sId + '_div_container',
						{ 
							iframe: false,
							hide_blank_weeks: true,
							pagedate: oMinDate,
							mindate: oMinDate,
							maxdate: oMaxDate
						});
					oCalendar.render();
					
					oCalendar.selectEvent.subscribe(function(p_sType, p_aArgs){
						var aDate;
						try {
						if (p_aArgs){
							aDate = p_aArgs[0][0];
							// get previous time
							var re = new RegExp(/(\d{2}:\d{2}:\d{2})/);
							var aTime = re.exec(YAHOO.util.Dom.get(p_sId + '_time').value);
							var sTime = '00:00:00';
							if (aTime){
								sTime = aTime[0];
							}
							var sNewDateTime = formatDateTimeAsISO(aDate[1] + '/' + aDate[2] + '/' + aDate[0] + ' ' + sTime);
							YAHOO.util.Dom.get(p_sId + '_time').value = sNewDateTime;
							YAHOO.ELSA.currentQuery.addMeta(p_sId + '_time', sNewDateTime);
						}
						} catch (e){ logger.log(e) }
						
						oDialog.hide();
					});
					
					// Hide Calendar if we click anywhere in the document other than the calendar
				    YAHOO.util.Event.on(document, "click", function(e) {
						var el = YAHOO.util.Event.getTarget(e);
						var oDialogEl = oDialog.element;
						
						if (el != oDialogEl 
							&& !YAHOO.util.	Dom.isAncestor(oDialogEl, el) 
							&& el != YAHOO.util.Dom.get('cal_' + p_sId + '_button') 
							&& !YAHOO.util.Dom.isAncestor(YAHOO.util.Dom.get('cal_' + p_sId + '_button'), el)) {
							oDialog.hide();
						}
					});
									
					oDialog.show();
				}	
				
				var oCalStartButtonConfig = { 
					type: 'button', 
					label: 'Start', 
					id: 'cal_start_button', 
					value: 'Start',
					onclick: {
						fn: function(){ onButtonClick('start'); }
					}
				};
				
				var oCalEndButtonConfig = { 
					type: 'button', 
					label: 'End', 
					id: 'cal_end_button', 
					value: 'End',
					onclick: {
						fn: function(){ onButtonClick('end'); }
					}
				};
				
				var oSubmitButtonConfig = { 
					type: "button", 
					label: "Submit Query", 
					id: "query_submit"
				};
				
				var onTermHostClick = function(p_sType, p_aArgs, p_oItem){
					logger.log('p_aArgs', p_aArgs);
					logger.log('p_oItem', p_oItem);
					var oPanel = new YAHOO.ELSA.Panel(this.element.id, {context:[this.element.id, 'tl', 'tr']});
					var elInput = document.createElement('input');
					elInput.type = 'text';
					elInput.id = 'add_term_' + this.element.id;
					oPanel.panel.setHeader('Host');
					oPanel.panel.body.appendChild(elInput);
					oPanel.panel.show();
					elInput.focus();
					var id = this.element.id;
					var enterKeyListener = new YAHOO.util.KeyListener(
							elInput,
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
									if (YAHOO.util.Dom.get('add_term_op_' + id)){
										op = YAHOO.util.Dom.get('add_term_op_' + id).value;
									}
									if (YAHOO.ELSA.currentQuery.addTerm('host', elInput.value, op, elInput)){
										oPanel.panel.hide();
									}
								},
								scope: YAHOO.ELSA,
								correctScope: false
							}
					);
					enterKeyListener.enable();
				}
				
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
					/*var div = document.createElement('div');
					div.id = 'target_' + this.element.id;
					YAHOO.util.Dom.get(this.element.id).appendChild(div);*/
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
						var aOps = ['=', '>=', '<='];
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
					oPanel.panel.show();
					elInput.focus();
					var enterKeyListener = new YAHOO.util.KeyListener(
							elInput,
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
									if (YAHOO.util.Dom.get('add_term_op_' + id)){
										op = YAHOO.util.Dom.get('add_term_op_' + id).value;
									}
									if (YAHOO.ELSA.currentQuery.addTerm(p_oItem.fqdn_field, elInput.value, op, elInput)){
										oPanel.panel.hide();
									}
								},
								scope: YAHOO.ELSA,
								correctScope: false
							}
					);
					enterKeyListener.enable();
				}
				
				// Build term selection menu
				var oFields = {
					'hosts': {
						text: 'Host',
						onclick: { 
							fn: onTermHostClick
						}
					},
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
					label: 'Add Term',
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
					YAHOO.ELSA.currentQuery.delMeta('local_groupby');
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
						// Only int field types can be grouped remotely in Sphinx
						if (sFieldType === 'int'){
							YAHOO.ELSA.currentQuery.addMeta('groupby', [aClassVal[1]]);
							YAHOO.ELSA.currentQuery.addMeta('groups_only', 1);
						}
						else {
							YAHOO.ELSA.currentQuery.addMeta('local_groupby', [aClassVal[1]]);
							//make sure we've got as many results to process as possible
							YAHOO.ELSA.currentQuery.addMeta('limit', YAHOO.ELSA.localGroupByQueryLimit);
						}
					}
				}
				
				var aUnclassedFields = ['Host', 'Class', 'Program', 'Day', 'Hour', 'Minute', 'Timestamp'];
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
						//id: 'groupby_Any',
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
					label: YAHOO.ELSA.Labels.noGroupBy,
					name: 'groupby_menu_select_button',
					menu: aGroupByMenuItems
				};
				
				var onRadioSelect = function(p_oEvent){
					//logger.log('p_oEvent', p_oEvent);
					YAHOO.ELSA.currentQuery.setBoolean(p_oEvent.newValue.get('name'));
				}
				var radioButtonCallback = function(p_oArgs, p_oInputEl, p_oEl){
					logger.log('p_oArgs', p_oArgs);
					logger.log('p_oInputEl', p_oInputEl);
					logger.log('p_oEl', p_oEl);
					var oButtonGroup = p_oInputEl;
					var aBooleans = ['OR', 'AND', 'NOT'];
					var aButtons = [];
					for (var i in aBooleans){
						var checked = false;
						if (aBooleans[i] == p_oArgs.args.checkedButton){
							checked = true;
						}
						aButtons.push({
							name: aBooleans[i],
							label: aBooleans[i],
							value: aBooleans[i],
							id: 'query_form_boolean_' + aBooleans[i].toLowerCase(),
							checked: checked
						});
					}
					logger.log('aButtons', aButtons);
					oButtonGroup.addButtons(aButtons);
					oButtonGroup.subscribe('checkedButtonChange', onRadioSelect);
					logger.log('oButtonGroup:', oButtonGroup);
				}
				
				var oArchiveButtonCfg = {
					id: 'archive_button',
					type: 'checkbox',
					label: 'Index',
					name: 'groupby_menu_select_button',
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
				var oFormGridCfg = {
					form_attrs:{
						id: 'query_menu'
					}
				};
				
				if (false){
				oFormGridCfg['grid'] = [
					[ {type:'text', args:'Query'}, {type:'input', args:{id:'q', size:80} }, {type:'widget', className:'Button', args:oSubmitButtonConfig} ],
					[ {type:'text', args:'Start Time'}, [ {type:'input', args:{id:'start_time', size:20, value:oFormParams.start}}, {type:'widget', className:'Button', args:oCalStartButtonConfig } ] ],
					[ {type:'text', args:'End Time'}, [ {type:'input', args:{id:'end_time', size:20/*, value:oFormParams.end*/}}, {type:'widget', className:'Button', args:oCalEndButtonConfig } ] ],
					[ {type:'text', args:'Add Search Field'}, [ {type:'widget', className:'Button', args:oTermMenuButtonCfg}, {type:'widget', className:'ButtonGroup', args:{id:'query_form_boolean', checkedButton:'OR'}, callback:radioButtonCallback }] ],
					[ {type:'text', args:'Group By'}, {type:'widget', className:'Button', args:oGroupByMenuButtonCfg} ]
				];}
				
				oFormGridCfg['grid'] = [
					[ 
						{type:'text', args:'Query'},
						{type:'input', args:{id:'q', size:80} }, 
						{type:'widget', className:'Button', args:oSubmitButtonConfig} 
					]
				];
				
				var oFormGridCfg2 = {
					form_attrs:{
						id: 'query_menu'
					}
				};
				
				oFormGridCfg2['grid'] = [
					[ 
						{type:'text', args:'Start Time'}, 
						{type:'input', args:{id:'start_time', size:20, value:oFormParams.start}}, 
						{type:'widget', className:'Button', args:oCalStartButtonConfig },
						{type:'text', args:'Add Term'},  
						{type:'widget', className:'ButtonGroup', args:{id:'query_form_boolean', checkedButton:'OR'}, callback:radioButtonCallback },
						{type:'text', args:'Group By'}, 
						{type:'widget', className:'Button', args:oGroupByMenuButtonCfg}
					],
					[
						{type:'text', args:'End Time'}, 
						{type:'input', args:{id:'end_time', size:20}}, 
						{type:'widget', className:'Button', args:oCalEndButtonConfig },
						{type:'text', args:' '}, 
						{type:'widget', className:'Button', args:oTermMenuButtonCfg},
						{type:'text', args:'Use'},
						{type:'widget', className:'Button', args:oArchiveButtonCfg}
					]
				];
				
				
				try {
					
					var oTargetForm = document.createElement('form');
					YAHOO.ELSA.queryForm = new YAHOO.ELSA.Form(oTargetForm, oFormGridCfg);
					var new2 = new YAHOO.ELSA.Form(oTargetForm, oFormGridCfg2);
					
					YAHOO.util.Dom.get('query_form').appendChild(oTargetForm);
					
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
					var str;
					for (var i in e){
						str = str + i + ' ' + e[i];
					}
					YAHOO.ELSA.Error('Error drawing query grid:' + str);
					return;	
				}
								
			}
			else {
				YAHOO.ELSA.Error('Did not receive form params');
				return false;
			}
		}
		catch(e){
			logger.log('error', e.stack);
			YAHOO.ELSA.Error('Error drawing query form:' + e);
			return;
		}
		
		/* Have the enter key submit the form */
		var enterKeyListener = new YAHOO.util.KeyListener(
				//YAHOO.util.Dom.get('query_form'),
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
							text: 'Previous Queries',
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
							text: 'Scheduled Queries',
							helptext: 'Currently scheduled queries',
							onclick: {
								fn: YAHOO.ELSA.getQuerySchedule
							}
						},
						{
							text: 'Running Queries',
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
		
		/*
		// Draw previous search link
		var oEl = document.createElement('a');
		oEl.innerHTML = 'Previous Queries';
		oEl.setAttribute('href', '#');
		oEl.id = 'prevSearchLink';
		YAHOO.util.Dom.get('menu_bar').appendChild(oEl);
		YAHOO.util.Event.addListener('prevSearchLink', 'click', YAHOO.ELSA.getPreviousQueries);
		
		// Draw saved search link
		var oEl = document.createElement('a');
		oEl.innerHTML = 'Saved Queries';
		oEl.setAttribute('href', '#');
		oEl.id = 'savedSearchLink';
		YAHOO.util.Dom.get('menu_bar').appendChild(oEl);
		YAHOO.util.Event.addListener('savedSearchLink', 'click', YAHOO.ELSA.getSavedQueries);
		*/
	}
	
	drawMenuBar();
	
	/* Get form params (goes all the way to a backend node) */
//	var request = YAHOO.util.Connect.asyncRequest('GET', 'Query/get_form_params', 
//		{ success:drawQueryForm, failure:drawQueryForm } );
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
		if (iLocalResultId){
			try {
				logger.log('parsing ' + YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
				var oQuery = YAHOO.lang.JSON.parse(YAHOO.ELSA.localResults[iLocalResultId].sentQuery);
			}
			catch (e){
				logger.log('error getting query for results:', e);
				logger.log('results:', YAHOO.ELSA.localResults[iLocalResultId]);
				return;
			}
			logger.log('set active query: ', oQuery);
			// set the q bar
			YAHOO.util.Dom.get('q').value = oQuery.query_params;
			
			//set the groupby button
			var oButton = YAHOO.widget.Button.getButton('groupby_menu_select_button');
			if (oQuery.query_meta_params){
				YAHOO.ELSA.currentQuery.metas = oQuery.query_meta_params;
				logger.log('current query: ' + YAHOO.lang.JSON.stringify(YAHOO.ELSA.currentQuery));
				logger.log('type of class: ' + typeof YAHOO.ELSA.currentQuery.metas['class']);
				logger.log('current groupby:', YAHOO.ELSA.currentQuery.metas.groupby);
				if (YAHOO.ELSA.currentQuery.metas.groupby){
					if (typeof YAHOO.ELSA.currentQuery.metas['class'] != 'undefined'){
						oButton.set('label', YAHOO.ELSA.currentQuery.metas['class'] + '.' + YAHOO.ELSA.currentQuery.metas.groupby);
					}
					else {
						oButton.set('label', 'any.' + YAHOO.ELSA.currentQuery.metas.groupby);
					}
				}
				else if (YAHOO.ELSA.currentQuery.metas.local_groupby){
					//TODO loop this instead of defaulting to 0 in array
					if (typeof YAHOO.ELSA.currentQuery.metas['class'] != 'undefined'){
						oButton.set('label', YAHOO.ELSA.currentQuery.metas['class'] + '.' + YAHOO.ELSA.currentQuery.metas.local_groupby[0]);
					}
					else {
						oButton.set('label', 'any.' + YAHOO.ELSA.currentQuery.metas.local_groupby[0]);
					}
				}
				else {
					oButton.set('label', YAHOO.ELSA.Labels.noGroupBy);
				}
				// set times
				if (YAHOO.ELSA.currentQuery.metas.start){
					YAHOO.util.Dom.get('start_time').value = getISODateTime(new Date(YAHOO.ELSA.currentQuery.metas.start * 1000));
				}
				if (YAHOO.ELSA.currentQuery.metas.end){
					YAHOO.util.Dom.get('end_time').value = getISODateTime(new Date(YAHOO.ELSA.currentQuery.metas.end * 1000));
				}
			}
			else {
				oButton.set('label', YAHOO.ELSA.Labels.noGroupBy);
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

