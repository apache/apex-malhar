var _ = require('underscore'); exports = module.exports = {"/datatorrent/App.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div id="header"></div>\n<div id="main"></div>';
}
return __p;
},"/datatorrent/DagOperatorView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="inner">\n    <span class="operator-label" title="'+
((__t=( tooltip ))==null?'':__t)+
'">\n        '+
((__t=( label ))==null?'':__t)+
'\n        ';
 if (name) { 
__p+='\n            \n        ';
 } else { 
__p+='\n            \n        ';
 } 
__p+='\n    </span>\n</div>';
}
return __p;
},"/datatorrent/ModalView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="modal-header">\n  <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>\n  <h3>'+
((__t=( title ))==null?'':__t)+
'</h3>\n</div>\n<div class="modal-body">\n  <p>'+
((__t=( body ))==null?'':__t)+
'</p>\n</div>\n<div class="modal-footer">\n  <a href="#" class="cancelBtn btn">'+
((__t=( cancelText ))==null?'':__t)+
'</a>\n  <a href="#" class="confirmBtn btn btn-primary">'+
((__t=( confirmText ))==null?'':__t)+
'</a>\n</div>';
}
return __p;
},"/datatorrent/WidgetHeaderView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="widget-ctrls">\n    <i class="icon-minus wi-toggle-collapse" title="minimize/maximize toggle (double-click top bar for same thing)"></i>\n    <i class="icon-cog wi-settings" title="edit settings for this widget"></i>\n    <i class="icon-remove wi-delete" title="remove this widget from the dashboard (double-click to suppress warning)"></i>\n</div>\n<span class="wid">'+
((__t=( id ))==null?'':__t)+
'</span> <span class="widget-type">'+
((__t=( widget ))==null?'':__t)+
'</span>';
}
return __p;
},"/datatorrent/WidgetSettingsView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="modal-header">\n    <h3 class="widget-settings-title"><span class="muted">Widget Settings:</span> '+
((__t=( widget ))==null?'':__t)+
'</h3>\n</div>\n<div class="modal-body">\n    <form>\n      <fieldset>\n        <label>Name</label>\n        <input type="text" value="'+
((__t=( id ))==null?'':__t)+
'" class="widgetId">\n        \n        <label>Width (in percentage)</label>\n        <div class="input-append">\n            <input type="text" value="'+
((__t=( width ))==null?'':__t)+
'" class="widgetWidth">\n            <span class="add-on">%</span>\n        </div>\n      </fieldset>\n    </form>\n</div>\n<div class="modal-footer">\n    <button class="btn btn-primary" data-dismiss="modal" aria-hidden="true">Close (esc)</button>\n</div>';
}
return __p;
},"/datatorrent/WidgetView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="widget-header"></div>\n<div class="widget-content '+
((__t=( contentClass ))==null?'':__t)+
'">'+
((__t=( content ))==null?'':__t)+
'</div>\n<div class="widget-resize"></div>';
}
return __p;
},"/datatorrent/Breadcrumbs/Breadcrumbs.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 _.each(crumbs, function(crumb) {
__p+='\n    ';
 if (crumb.href) {
__p+='\n        <a class="breadcrumb" href="'+
((__t=( crumb.href ))==null?'':__t)+
'">'+
((__t=( crumb.name ))==null?'':__t)+
'</a> \n    ';
 } else { 
__p+='\n        <span class="breadcrumb">'+
((__t=( crumb.name ))==null?'':__t)+
'</span>    \n    ';
 }
__p+='\n';
 }) 
__p+='';
}
return __p;
},"/datatorrent/DashMgrView/DashActions.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<button class="btn btn-mini createDash">create new</button>';
}
return __p;
},"/datatorrent/DashMgrView/DashItem.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<input type="radio" name="curDashRadio" value="'+
((__t=(dash_id))==null?'':__t)+
'" id="dash-'+
((__t=(dash_id))==null?'':__t)+
'" ';
 if (selected) { print('checked') }
__p+=' />\n<label for="dash-'+
((__t=(dash_id))==null?'':__t)+
'" class="dash_id">\n    ';
 if (selected) { 
__p+='\n    <span class="bullet">&bull;</span>\n    ';
 } 
__p+='\n    '+
((__t=(dash_id))==null?'':__t)+
'\n</label>\n';
 if (!isDefault) {
__p+='\n\t<i class="icon-pencil editDashName"></i>\n    <i class="icon-trash removeDash"></i>\n';
}
__p+='';
}
return __p;
},"/datatorrent/DashMgrView/DashList.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<h2 class="title">dashboards</h2><ul></ul><div class="actions"></div>';
}
return __p;
},"/datatorrent/DashMgrView/DashMgr.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="inner">\n    <div class="widget-list"></div>\n    <div class="dash-list"></div>\n</div>\n<div class="collapser-container">\n    <div class="collapser">\n        <div class="collapse-dot"></div>\n        <div class="collapse-dot"></div>\n        <div class="collapse-dot"></div>\n    </div>\n</div>';
}
return __p;
},"/datatorrent/DashMgrView/WidgetList.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<h2 class="title">widgets</h2>\n<ul class="widget-instances"></ul>\n<div class="actions btn-group">\n    <a href="#" class="btn btn-mini dropdown-toggle" data-toggle="dropdown">\n    \tadd widget\n    \t<span class="caret"></span>\n    </a>\n    <ul class="dropdown-menu">\n    \t';
 _.each(wClasses, function(wc){ 
__p+='\n    \t\t<li><a href="#" data-wname="'+
((__t=( wc.name ))==null?'':__t)+
'" class="addWidgetOption">'+
((__t=( wc.name ))==null?'':__t)+
'</a></li>\n    \t';
 }) 
__p+='\n    </ul>\n</div>';
}
return __p;
},"/datatorrent/DashMgrView/WidgetListItem.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<i class="icon-list wi-handle" title="click and drag to change order"></i>\n<span class="wid" title="'+
((__t=(widget))==null?'':__t)+
'">'+
((__t=(id))==null?'':__t)+
'</span>\n<i class="icon-cog wi-settings" title="edit settings for this widget"></i>\n<i class="icon-remove wi-delete" title="remove this widget from the dashboard (double-click to suppress warning)"></i>';
}
return __p;
},"/datatorrent/HeaderView/HeaderView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div id="header">\n    <a href="#" class="brand">DataTorrent</a> <!-- platform logo -->\n    <div class="ui-version" title="User Interface Version">\n        ';
 if (version) { 
__p+='\n            v'+
((__t=( version ))==null?'':__t)+
'\n        ';
 } 
__p+='\n    </div>\n    <a id="feedback-header-link" class="btn btn-mini btn-success" href="mailto:support@datatorrent.com">Send Feedback</a>\n    <!-- sign in area -->\n    <div class="sign-in-container" style="display:none;">\n        <a href="#sign-in"></a>\n    </div>\n    <ul class="dashboard-modes">\n        ';
 _.each(modes, function(mode) { 
__p+='\n            <li>\n                <a href="'+
((__t=( mode.href ))==null?'':__t)+
'" class="'+
((__t=( mode.class ))==null?'':__t)+
'">'+
((__t=( mode.name ))==null?'':__t)+
'</a>\n            </li\n        >';
 }) 
__p+='\n    </ul>\n    <div class="client-brand">\n        <!-- <img src="img/'+
((__t=( client_logo ))==null?'':__t)+
'"> -->\n    </div>\n</div>';
}
return __p;
},"/datatorrent/LaunchJarWithPropsView/LaunchJarWithPropsView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<table class="properties">\n\t<thead>\n\t\t<tr>\n\t\t\t<th scope="col">property name</th>\n\t\t\t<th scope="col">property value</th>\n\t\t\t<th scope="col">remove</th>\n\t\t</tr>\n\t</thead>\n\t<tbody>\n\t\t';
 if (properties.length === 0) { 
__p+='\n\t\t\t<tr><td colspan="2" class="no-properties">'+
((__t=( DT.text('No properties specified.') ))==null?'':__t)+
'</td></tr>\n\t\t';
 } else { 
__p+='\n\t\t\t';
 _.each( properties, function(prop) { 
__p+='\n\t\t\t\t\n\t\t\t\t<tr>\n\t\t\t\t\t<td>\n\t\t\t\t\t\t<input \n\t\t\t\t\t\t\ttype="text"\n\t\t\t\t\t\t\tclass="propertyField"\n\t\t\t\t\t\t\tplaceholder="'+
((__t=( DT.text('property name') ))==null?'':__t)+
'"\n\t\t\t\t\t\t\tdata-id="'+
((__t=( prop.id ))==null?'':__t)+
'" \n\t\t\t\t\t\t\tdata-key="name"\n\t\t\t\t\t\t\tvalue="'+
((__t=( prop.name ))==null?'':__t)+
'"\n\t\t\t\t\t\t>\n\t\t\t\t\t</td>\n\t\t\t\t\t<td>\n\t\t\t\t\t\t<input \n\t\t\t\t\t\t\ttype="text" \n\t\t\t\t\t\t\tclass="propertyField" \n\t\t\t\t\t\t\tplaceholder="'+
((__t=( DT.text('property value') ))==null?'':__t)+
'"\n\t\t\t\t\t\t\tdata-id="'+
((__t=( prop.id ))==null?'':__t)+
'" \n\t\t\t\t\t\t\tdata-key="value"\n\t\t\t\t\t\t\tvalue="'+
((__t=( prop.value ))==null?'':__t)+
'"\n\t\t\t\t\t\t>\n\t\t\t\t\t</td>\n\t\t\t\t\t<td>\n\t\t\t\t\t\t<button class="btn btn-danger removeProperty" data-id="'+
((__t=( prop.id ))==null?'':__t)+
'">\n\t\t\t\t\t\t\t<i class="icon-white icon-remove"></i>\n\t\t\t\t\t\t</button>\n\t\t\t\t\t</td>\n\t\t\t\t</tr>\n\n\t\t\t';
 }) 
__p+='\n\t\t';
 } 
__p+='\n\t</tbody>\n</table>\n<button class="btn addProperty">'+
((__t=( DT.text('Add a property') ))==null?'':__t)+
'</button>';
}
return __p;
},"/datatorrent/PageLoaderView/PageLoaderView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div id="breadcrumbs"></div>\n<div id="pagecontent" class="';
 if (useDashMgr) { print('with-dash-manager') } 
__p+='"></div>';
}
return __p;
},"/datatorrent/templates/app_instance_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a title="'+
((__t=( appId ))==null?'':__t)+
'" href="#ops/apps/'+
((__t=( appId ))==null?'':__t)+
'">\n\t';
 
		if (typeof displayText !== 'undefined') { 
			print(displayText); 
		} else { 
			var parts = appId.split('_'); 
			if (parts.length) { 
				print(parts[parts.length - 1]) 
			} 
		} 
	
__p+='\n</a>';
}
return __p;
},"/datatorrent/templates/container_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 if (appId) { 
__p+='\n<a title="'+
((__t=( containerId ))==null?'':__t)+
'" href="#ops/apps/'+
((__t=( appId ))==null?'':__t)+
'/containers/'+
((__t=( containerId ))==null?'':__t)+
'">'+
((__t=( containerIdShort ))==null?'':__t)+
'</a>\n';
 } else { 
__p+='\n<span title="'+
((__t=( containerId ))==null?'':__t)+
'">'+
((__t=( containerIdShort ))==null?'':__t)+
'</span>\n';
 } 
__p+='';
}
return __p;
},"/datatorrent/templates/jar_app_view_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a href="#dev/jars/'+
((__t=( fileName ))==null?'':__t)+
'/'+
((__t=( encodeURIComponent(appName) ))==null?'':__t)+
'">'+
((__t=( appName ))==null?'':__t)+
'</a>';
}
return __p;
},"/datatorrent/templates/jar_view_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a href="#dev/jars/'+
((__t=( name ))==null?'':__t)+
'">'+
((__t=( name ))==null?'':__t)+
'</a>';
}
return __p;
},"/datatorrent/templates/phys_op_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a title="click to inspect this operator" href="#ops/apps/'+
((__t=( appId ))==null?'':__t)+
'/operators/'+
((__t=( operatorId ))==null?'':__t)+
'">'+
((__t=( displayText ))==null?'':__t)+
'</a>';
}
return __p;
},"/datatorrent/templates/port_name_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a href="#ops/apps/'+
((__t=( appId ))==null?'':__t)+
'/operators/'+
((__t=( operatorId ))==null?'':__t)+
'/ports/'+
((__t=( portName ))==null?'':__t)+
'">'+
((__t=( portName ))==null?'':__t)+
'</a>';
}
return __p;
},"/datatorrent/templates/recording_name_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 if (!ended) { 
__p+='\n<i class="icon-rec-on" title="indicates that this recording has not ended"></i>\n';
 } 
__p+='\n<a href="#ops/apps/'+
((__t=( appId ))==null?'':__t)+
'/operators/'+
((__t=( operatorId ))==null?'':__t)+
'/recordings/'+
((__t=( startTime ))==null?'':__t)+
'">'+
((__t=( recordingName ))==null?'':__t)+
'</a>';
}
return __p;
},"/datatorrent/templates/stream_name_link.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a href="#ops/apps/'+
((__t=( appId ))==null?'':__t)+
'/streams/'+
((__t=( streamName ))==null?'':__t)+
'">'+
((__t=( streamName ))==null?'':__t)+
'</a>';
}
return __p;
},"/datatorrent/widgets/ActionWidget/ActionWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
}
return __p;
},"/datatorrent/widgets/InfoWidget/InfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
}
return __p;
},"/datatorrent/widgets/ListWidget/ListPalette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<span class="select-status"><strong>'+
((__t=(selected.length))==null?'':__t)+
'</strong> '+
((__t=( DT.text('row(s) selected') ))==null?'':__t)+
'</span>\n<div class="palette-actions">\n\n\t';
 if (selected.length === 0) { 
__p+='\n\t\t<button class="btn" disabled>'+
((__t=( DT.text('select an item') ))==null?'':__t)+
'</button>\n\t';
 } else if (selected.length === 1) {
__p+='\n\t\t<button class="inspectItem btn btn-primary"><i class="icon-eye-open icon-white"></i> '+
((__t=( DT.text('inspect') ))==null?'':__t)+
'</button>\n\t';
 } else { 
__p+='\n\t\t<button class="btn" disabled>'+
((__t=( DT.text('no action available') ))==null?'':__t)+
'</button>\n\t';
 } 
__p+='\n    \n</div>';
}
return __p;
},"/datatorrent/widgets/ListWidget/ListWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="palette-target"></div><div class="table-target"></div>';
}
return __p;
},"/datatorrent/widgets/OverviewWidget/OverviewWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
}
return __p;
},"/datatorrent/widgets/TopNWidget/TopNWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form class="form-inline renderMode-form">\n    <div class="control-group renderMode-control">\n        <label for="renderMode">render mode:</label>\n        <select class="renderMode" name="renderMode">\n            <option value="pie">pie</option>\n            <!-- <option value="vbar">vertical bars</option> -->\n            <option value="hbar">horizontal bars</option>\n            <option value="table">table</option>\n        </select>\n    </div>\n</form>\n<div class="topn-visual"></div>';
}
return __p;
},"/datatorrent/widgets/TopNWidget/lib/bar.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="topn-legend"></div>\n<div class="bar-chart"></div>\n';
}
return __p;
},"/datatorrent/widgets/TopNWidget/lib/pie.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="pie-chart"></div>\n<div class="topn-legend"></div>';
}
return __p;
},"/app/lib/widgets/AddAlertTemplateWidget/AddAlertTemplateWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="widget-inner widget-procedure">\n    \n    <div class="title">\n        <span class="inner">Add an Alert</span>\n    </div>\n    <p class="description">Use this page to add an alert to '+
((__t=( app.id ))==null?'':__t)+
'.</p>\n    \n    \n    <div>\n        <div class="procedure-column">\n            \n            <form id="alert-form">\n                <div class="procedure-section">\n\n                    <h2>Name &amp; Target</h2>\n                    <div class="control-group">\n                        <label for="alertName">Alert Name</label>\n                        <input type="text" name="alertName" placeholder="letters, number, and underscores" id="alertName">\n                        <span class="help-block">Alert names may only contain letters, numbers, and underscores.</span>\n                    </div>\n                    <div class="control-group">\n                        <label for="saveAs">Save template as...</label>\n                        <input type="text" name="saveAs" placeholder="letters, number, and underscores" id="saveAs">\n                        <span class="help-block">Fill this in if you would like to save this alert as a template for future use. (optional)</span>\n                    </div>\n                    <div class="control-group">\n                        <label for="operatorName">Target Operator</label>\n                        <select id="operatorName">\n                            <option value="">choose target operator</option>\n                            ';
 _.each( app.logicalPlan.operators, function(operator,operatorName) { 
__p+='\n                                <option value="'+
((__t=( operatorName ))==null?'':__t)+
'" ';
 if (operatorName === alert.operatorName) { print('selected="selected"') } 
__p+='>'+
((__t=( operatorName ))==null?'':__t)+
'</option>\n                            ';
 }) 
__p+='\n                        </select>\n                        <span class="help-block">The alert will listen to a stream (or unconnected port) on this operator</span>\n                    </div>\n\n                    <div class="control-group streamName">\n\n                    </div>\n                </div>\n\n                <div class="procedure-section">\n                    <h2>Condition</h2>\n                    <p>Set the condition in which an alert should be triggered.</p>\n                    <div id="filter-section"></div>\n                </div>\n\n                <div class="procedure-section">\n                    <h2>Escalation</h2>\n                    <p>Specify the escalation rules here.</p>\n                    <div id="escalation-section"></div>\n                </div>\n\n                <div class="procedure-section">\n                    <h2>Action(s)</h2>\n                    <p>Specify up to '+
((__t=( settings.maxAlertActions ))==null?'':__t)+
' actions.</p>\n                    <div id="action-section">\n                        ';
 for (var i = 1, max = settings.maxAlertActions; i <= max; i++) { 
__p+='\n                        <div class="alertAction'+
((__t=( i ))==null?'':__t)+
'">\n                            <h3>action '+
((__t=( i ))==null?'':__t)+
'</h3>\n                        </div>\n                        ';
 } 
__p+='\n                    </div>\n                </div>\n                \n                <input type="submit" value="add alert" id="alert_submit_btn" class="btn btn-primary">\n                \n            </form>\n            \n        </div\n\n        ><div class="visual-column">\n            <div class="procedure-section">\n                <h2>Visualization</h2>\n                <div class="dag-container">\n                \n                </div>\n            </div>\n        </div>\n        \n    </div>\n    \n</div>';
}
return __p;
},"/app/lib/widgets/AddAlertTemplateWidget/BaseSubView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="operator-class-definition">\n    ';
 if (classes.length) {
__p+='\n    <select class="classSelect">\n        <option value="">choose a class</option>\n\n        ';
 _.each(classes, function(classObj) { 
__p+='\n        <option value="'+
((__t=( classObj.name ))==null?'':__t)+
'" ';
 if (classObj.name === alert.name) { print('selected="selected"') } 
__p+='>'+
((__t=( classObj.name ))==null?'':__t)+
'</option>\n        ';
 }) 
__p+='\n\n    ';
} else { 
__p+='\n    <select class="classSelect" disabled="disabled">\n        <option>loading classes</option>\n\n    ';
 } 
__p+='\n\n    </select>\n\n    <div class="propertyDefinitions">\n\n    </div>\n</div>';
}
return __p;
},"/app/lib/widgets/AddAlertTemplateWidget/ClassPropertiesView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 _.each( properties, function(property) { 
__p+='\n    <div class="control-group">\n        <label for="property_'+
((__t=( property.name ))==null?'':__t)+
'">'+
((__t=( property.name ))==null?'':__t)+
' <span class="property-type-span">(property type: '+
((__t=( property.class ))==null?'':__t)+
')</span></label>\n        ';
 switch(property.class) {
            case "java.lang.String":
                
__p+='\n                <textarea placeholder="'+
((__t=( property.description ))==null?'':__t)+
'" data-property="'+
((__t=( property.name ))==null?'':__t)+
'" data-javatype="'+
((__t=( property.class ))==null?'':__t)+
'"></textarea>\n                ';

            break;
        
            case "boolean":
                
__p+='\n                <input type="checkbox" data-property="'+
((__t=( property.name ))==null?'':__t)+
'" title="'+
((__t=( property.description ))==null?'':__t)+
'" data-javatype="'+
((__t=( property.class ))==null?'':__t)+
'" />\n                ';

            break;
        
            case "long":
                
__p+='\n                <input type="text" data-property="'+
((__t=( property.name ))==null?'':__t)+
'" value="" placeholder="'+
((__t=( property.description ))==null?'':__t)+
'" data-javatype="'+
((__t=( property.class ))==null?'':__t)+
'" />\n                ';

            break;
            
            case "double":
                
__p+='\n                <input type="text" data-property="'+
((__t=( property.name ))==null?'':__t)+
'" value="" placeholder="'+
((__t=( property.description ))==null?'':__t)+
'" data-javatype="'+
((__t=( property.class ))==null?'':__t)+
'" />\n                ';

            break;
            
            case "int":
                
__p+='\n                <input type="text" data-property="'+
((__t=( property.name ))==null?'':__t)+
'" value="" placeholder="'+
((__t=( property.description ))==null?'':__t)+
'" data-javatype="'+
((__t=( property.class ))==null?'':__t)+
'" />\n                ';

            break;
            
            default:
                
__p+='\n                <textarea placeholder="'+
((__t=( property.description ))==null?'':__t)+
'" data-property="'+
((__t=( property.name ))==null?'':__t)+
'" data-javatype="'+
((__t=( property.class ))==null?'':__t)+
'"></textarea>\n                ';

            break;
            
        }; 
__p+='\n    </div>\n';
 }) 
__p+='';
}
return __p;
},"/app/lib/widgets/AddAlertTemplateWidget/TargetStreamView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<label class="control-label" for="streamName">\n    Target Stream\n    <span class="help-block">The alert will listen to this stream</span>\n</label>\n<div class="controls">\n    ';
 if (streamOptions) {
__p+='\n        <select id="streamName">\n            <option value="">choose a stream</option>\n            ';
 if (streamOptions.streams.length) {
__p+='\n                <optgroup label="connected streams">\n                ';
 _.each( streamOptions.streams, function(stream) { 
__p+='\n                    <option value="'+
((__t=( stream ))==null?'':__t)+
'">'+
((__t=( stream ))==null?'':__t)+
'</option>\n                ';
 }) 
__p+='\n                </optgroup>\n            ';
} 
__p+='\n\n            ';
 if (streamOptions.ports.length) {
__p+='\n                <optgroup label="disconnected ports">\n                ';
 _.each( streamOptions.ports, function(port) { 
__p+='\n                    <option value="'+
((__t=( port ))==null?'':__t)+
'">'+
((__t=( port ))==null?'':__t)+
'</option>\n                ';
 }) 
__p+='\n                </optgroup>\n            ';
} 
__p+='\n        </select>\n    ';
} else {
__p+='\n        <select id="streamName" disabled="disabled">\n            <option value="">no stream options</option>\n        </select>\n    ';
}
__p+='\n</div> ';
}
return __p;
},"/app/lib/widgets/AddAlertWidget/AddAlertWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="widget-inner widget-procedure">\n    \n    <div class="title">\n        <span class="inner">Add an Alert</span>\n    </div>\n    <p class="description">Use this page to add an alert to <strong>'+
((__t=( app.name ))==null?'':__t)+
'</strong> <br>( id: '+
((__t=( app.name ))==null?'':__t)+
' ).</p>\n    \n    \n    <div class="procedure-column-container">\n        <div class="procedure-column">\n            \n            <form id="alert-form" class="form-horizontal">\n                \n                <div class="procedure-section">\n\n                    <h2>Name &amp; Target Stream</h2>\n                    <div class="control-group">\n                        <label class="control-label" for="alertName">\n                            Alert Name\n                            <span class="help-block">Alert names may only contain letters, numbers, and underscores.</span>\n                        </label>\n                        <div class="controls">\n                            <input type="text" name="alertName" placeholder="letters, number, and underscores" id="alertName">\n                        </div>\n                    </div>\n                    <div class="control-group">\n                        <label class="control-label" for="operatorName">\n                            Target Operator\n                            <span class="help-block">The alert will listen to a stream (or unconnected port) on this operator</span>\n                        </label>\n                        <div class="controls">\n                            <select id="operatorName">\n                                <option value="">choose target operator</option>\n                                ';
 _.each( app.logicalPlan.get('operators'), function(operator,operatorName) { 
__p+='\n                                    <option value="'+
((__t=( operatorName ))==null?'':__t)+
'" ';
 if (operatorName === alert.operatorName) { print('selected="selected"') } 
__p+='>'+
((__t=( operatorName ))==null?'':__t)+
'</option>\n                                ';
 }) 
__p+='\n                            </select>\n                        </div>\n                    </div>\n\n                    <div class="control-group streamName">\n\n                    </div>\n                    \n                    <div class="control-group templateName">\n                        \n                    </div>\n                    \n                </div>\n                \n                <div class="procedure-section">\n                    \n                    <h2>Parameters</h2>\n                    \n                    <div class="templateParams"></div>\n                    \n                </div>\n\n                <input type="submit" value="add alert" id="alert_submit_btn" class="btn btn-primary">\n                \n            </form>\n            \n        </div>\n        \n    </div>\n    \n</div>';
}
return __p;
},"/app/lib/widgets/AddAlertWidget/TargetStreamView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<label class="control-label" for="streamName">\n    Target Stream\n    <span class="help-block">The alert will listen to this stream</span>\n</label>\n<div class="controls">\n    ';
 if (streamOptions) {
__p+='\n        <select id="streamName">\n            <option value="">choose a stream</option>\n            ';
 if (streamOptions.streams.length) {
__p+='\n                <optgroup label="connected streams">\n                ';
 _.each( streamOptions.streams, function(stream) { 
__p+='\n                    <option value="'+
((__t=( stream ))==null?'':__t)+
'">'+
((__t=( stream ))==null?'':__t)+
'</option>\n                ';
 }) 
__p+='\n                </optgroup>\n            ';
} 
__p+='\n\n            ';
 if (streamOptions.ports.length) {
__p+='\n                <optgroup label="disconnected ports">\n                ';
 _.each( streamOptions.ports, function(port) { 
__p+='\n                    <option value="'+
((__t=( port ))==null?'':__t)+
'">'+
((__t=( port ))==null?'':__t)+
'</option>\n                ';
 }) 
__p+='\n                </optgroup>\n            ';
} 
__p+='\n        </select>\n    ';
} else {
__p+='\n        <select id="streamName" disabled="disabled">\n            <option value="">no stream options</option>\n        </select>\n    ';
}
__p+='\n</div> \n';
}
return __p;
},"/app/lib/widgets/AddAlertWidget/TemplateParamsView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 if (!template) { 
__p+='\n    \n    first choose a template\n    \n';
 } else if (!template.parameters.length) { 
__p+='\n    \n    template loading...\n    \n';
 } else { 
__p+='\n    \n    ';
 _.each( template.parameters, function(param) { 
__p+='\n        \n        <div class="control-group">\n            <label class="control-label" for="param_'+
((__t=( param.name ))==null?'':__t)+
'">\n                '+
((__t=( param.name ))==null?'':__t)+
'\n                <span class="help-block">'+
((__t=( param.description ))==null?'':__t)+
'</span>\n            </label>\n            \n            <div class="controls">\n            ';
 switch(param.type) {
            
                case 'string':
                    
__p+='\n                    <textarea name="param_'+
((__t=( param.name ))==null?'':__t)+
'" placeholder="string"></textarea>\n                    ';

                    break;
            
                case 'int':
                    
__p+='\n                    <input type="text" name="param_'+
((__t=( param.name ))==null?'':__t)+
'" placeholder="integer">\n                    ';

                    break;
                
                case 'float':
                    
__p+='\n                    <input type="text" name="param_'+
((__t=( param.name ))==null?'':__t)+
'" placeholder="float">\n                    ';

                    break;
                
                case 'enum':
                    
__p+='\n                    <select name="param_'+
((__t=( param.name ))==null?'':__t)+
'">\n                        <option value="">choose one</option>\n                        ';
 _.each( param.options, function(option) { 
__p+='\n                            <option value="'+
((__t=( option ))==null?'':__t)+
'">'+
((__t=( option ))==null?'':__t)+
'</option>\n                        ';
 }) 
__p+='\n                    </select>\n                    ';

                    break;
            
            } 
__p+='\n            </div>\n        </div>\n        \n    ';
 }) 
__p+='\n    \n';
 } 
__p+='';
}
return __p;
},"/app/lib/widgets/AddAlertWidget/TemplateSelectView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<label class="control-label" for="templateName">\n    Template Name\n    <span class="help-block">The alert will listen to this stream</span>\n</label>\n    \n<div class="controls">\n    ';
 if (templates.length) {
__p+='\n        <select id="templateName" name="templateName">\n            <option value="">choose an alert template</option>\n            ';
 _.each( templates, function(template) { 
__p+='\n            <option value="'+
((__t=( template.name ))==null?'':__t)+
'">'+
((__t=( template.name ))==null?'':__t)+
'</option>\n            ';
 }) 
__p+='\n        </select>\n    ';
} else {
__p+='\n        <select id="templateName" disabled="disabled">\n            <option value="">no template options</option>\n        </select>\n    ';
}
__p+='\n</div>';
}
return __p;
},"/app/lib/widgets/AlertListWidget/Palette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="palette-actions">\n    <button class="btn btn-primary alert-list-add">add</button>\n';
 if (one) { 
__p+='\n    <button class="btn btn-primary alert-list-delete">delete</button>\n';
 } 
__p+='\n    <button class="btn btn-info alert-list-refresh"><i class="icon-refresh icon-white"></i> refresh list</button>\n</div>';
}
return __p;
},"/app/lib/widgets/AppDagWidget/AppDagWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="app-dag">\n    <svg height="80">\n        <g transform="translate(20, 20)"/>\n    </svg>\n</div>\n';
}
return __p;
},"/app/lib/widgets/AppListWidget/AppListPalette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<span class="select-status"><strong>'+
((__t=( selected.length ))==null?'':__t)+
'</strong> '+
((__t=( DT.text('row(s) selected') ))==null?'':__t)+
'</span>\n<div class="palette-actions">\n\n    ';
 if (selected.length === 0) { 
__p+='\n        <button class="btn" disabled>'+
((__t=( DT.text('select an item') ))==null?'':__t)+
'</button>\n    ';
 } else if (selected.length === 1) {
__p+='\n        <button class="inspectApp btn btn-primary"><i class="icon-eye-open icon-white"></i> '+
((__t=( DT.text('inspect') ))==null?'':__t)+
'</button>\n        ';
 if (selected[0]['state'] === 'RUNNING') {
__p+='\n            <button class="shutdownApp btn btn-warning"><i class="icon-white icon-remove"></i> shutdown</button>\n            <button class="killApp btn btn-danger"><i class="icon-white icon-ban-circle"></i> kill</button>\n        ';
 } else { 
__p+='\n            <!-- <button class="btn">relaunch</button> -->\n        ';
 } 
__p+='\n    ';
 } else {
__p+='\n        <button class="shutdownApps btn btn-warning"><i class="icon-white icon-remove"></i> shutdown selected</button>\n        <button class="killApps btn btn-danger"><i class="icon-white icon-ban-circle"></i> kill selected</button>\n    ';
 } 
__p+='\n    \n</div>';
}
return __p;
},"/app/lib/widgets/AppMetricsWidget/Ctrl.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form class="form-horizontal">\n    <h4>Controls</h4>\n    <label for="toggle_multi_y" class="checkbox">\n        <input type="checkbox" class="toggle_multi_y" name="toggle_multi_y" ';
 if (multi_y) { print('checked ')} 
__p+='/> Multiple Y-Axes\n    </label>\n    \n    ';
 _.each( plots, function(plot) { 
__p+='\n        <label class="checkbox">\n            <input type="checkbox" data-key="'+
((__t=( plot.key ))==null?'':__t)+
'" />\n            <span class="plot-colorblock" style="background-color:'+
((__t=( plot.color ))==null?'':__t)+
'"></span>\n            '+
((__t=( plot.label ))==null?'':__t)+
'\n        </label>\n    ';
 }) 
__p+='\n    \n</form>';
}
return __p;
},"/app/lib/widgets/ClusterOverviewWidget/ClusterOverviewWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<button class="btn refreshCluster"><i class="icon-refresh"></i></button>\n<div class="overview-item">\n    <span class="key">avg app age</span>\n    <span class="value">'+
((__t=( DT.formatters.timeSince({ timeChunk: averageAge }) || '-' ))==null?'':__t)+
'</span>\n</div>\n<div class="overview-item">\n    <span class="key">CPU</span>\n    <span class="value">'+
((__t=( DT.formatters.percentageFormatter(cpuPercentage, true) ))==null?'':__t)+
'</span>\n</div>\n<div class="overview-item">\n    <span class="key">current/max allocated mem (MB)</span>\n    <span class="value">'+
((__t=( DT.formatters.commaGroups(currentMemoryAllocatedMB) ))==null?'':__t)+
' / '+
((__t=( DT.formatters.commaGroups(maxMemoryAllocatedMB) ))==null?'':__t)+
'</span>\n</div>\n<div class="overview-item">\n    <span class="key">running / pending / failed / finished / killed / submitted</span>\n    <span class="value">\n        <span class="status-running">'+
((__t=( DT.formatters.commaGroups(numAppsRunning)))==null?'':__t)+
'</span> / \n        <span class="status-pending-deploy">'+
((__t=( DT.formatters.commaGroups(numAppsPending)))==null?'':__t)+
'</span> / \n        <span class="status-failed">'+
((__t=( DT.formatters.commaGroups(numAppsFailed)))==null?'':__t)+
'</span> / \n        <span class="status-finished">'+
((__t=( DT.formatters.commaGroups(numAppsFinished)))==null?'':__t)+
'</span> / \n        <span class="status-killed">'+
((__t=( DT.formatters.commaGroups(numAppsKilled)))==null?'':__t)+
'</span> / \n        <span class="status-submitted">'+
((__t=( DT.formatters.commaGroups(numAppsSubmitted)))==null?'':__t)+
'</span>\n    </span>\n</div>\n<div class="overview-item">\n    <span class="key">no. containers</span>\n    <span class="value">'+
((__t=( DT.formatters.commaGroups(numContainers)))==null?'':__t)+
'</span>\n</div>\n<div class="overview-item">\n    <span class="key">no. operators</span>\n    <span class="value">'+
((__t=( DT.formatters.commaGroups(numOperators)))==null?'':__t)+
'</span>\n</div>\n<div class="overview-item">\n    <span class="key">processed/s</span>\n    <span class="value">'+
((__t=( DT.formatters.commaGroups(tuplesProcessedPSMA)))==null?'':__t)+
'</span>\n</div>\n<div class="overview-item">\n    <span class="key">emitted/s</span>\n    <span class="value">'+
((__t=( DT.formatters.commaGroups(tuplesEmittedPSMA)))==null?'':__t)+
'</span>\n</div>';
}
return __p;
},"/app/lib/widgets/CtnrInfoWidget/CtnrInfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="right-top info-item">\n    <span class="key">As Of:</span>\n    <span class="value">'+
((__t=(as_of))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n    <span class="key">Container Id:</span>\n    <span class="value">'+
((__t=(id))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n    <span class="key">Host:</span>\n    <span class="value">'+
((__t=(host))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n    <span class="key">JVM Name:</span>\n    <span class="value">'+
((__t=(jvmName))==null?'':__t)+
'</span>\n</div>';
}
return __p;
},"/app/lib/widgets/CtnrMetricsWidget/Ctrl.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form class="form-horizontal well">\n    <h4>Controls</h4>\n    <label for="toggle_multi_y" class="checkbox">\n        <input type="checkbox" class="toggle_multi_y" name="toggle_multi_y" ';
 if (multi_y) { print('checked ')} 
__p+='/> Multiple Y-Axes\n    </label>\n    \n    ';
 _.each( plots, function(plot) { 
__p+='\n        <label class="checkbox">\n            <input type="checkbox" data-key="'+
((__t=( plot.key ))==null?'':__t)+
'" />\n            <span class="plot-colorblock" style="background-color:'+
((__t=( plot.color ))==null?'':__t)+
'"></span>\n            '+
((__t=( plot.label ))==null?'':__t)+
'\n        </label>\n    ';
 }) 
__p+='\n    \n</form>';
}
return __p;
},"/app/lib/widgets/CtnrOverviewWidget/CtnrOverviewWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="overview-item">\n    <div class="key">State</div>\n    <div class="value status-';
 print(state.toLowerCase())
__p+='">'+
((__t=( state ))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Allocated Memory (mb)</div>\n    <div class="value">'+
((__t=( memoryMBAllocated ))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Free Memory (mb)</div>\n    <div class="value">'+
((__t=( memoryMBFree ))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Operators</div>\n    <div class="value">'+
((__t=( numOperators ))==null?'':__t)+
'</div>\n</div>';
}
return __p;
},"/app/lib/widgets/InstanceActionWidget/default.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<button class="btn" disabled="disabled">no available actions</button>';
}
return __p;
},"/app/lib/widgets/InstanceActionWidget/running.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a href="#ops/apps/'+
((__t=( id ))==null?'':__t)+
'/add_alert" class="btn btn-info"><i class="icon-white icon-bell"></i> add an alert</a>\n<button class="btn btn-warning shutdownApplication"><i class="icon-white icon-remove"></i> shutdown</button>\n<button class="btn btn-danger killApplication"><i class="icon-white icon-ban-circle"></i> kill</button>';
}
return __p;
},"/app/lib/widgets/InstanceInfoWidget/InstanceInfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="info-item">\n    <span class="key">name:</span>\n    <span class="value">'+
((__t=(name))==null?'':__t)+
'</span>\n</div>\n    \n<div class="info-item">\n    <span class="key">id:</span>\n    <span class="value">'+
((__t=(idShorthand))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n    <span class="key">user:</span>\n    <span class="value">'+
((__t=(user))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n\t<span class="key">version:</span>\n\t<span class="value app-version" title="'+
((__t=( version ))==null?'':__t)+
'">'+
((__t=( version.replace(/^([^\s]+).*$/, '$1') ))==null?'':__t)+
'</span>\n</div>';
}
return __p;
},"/app/lib/widgets/InstanceOverviewWidget/InstanceOverviewWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<!-- state -->\n<div class="overview-item">\n    <div class="key">state</div>\n    <div class="value '+
((__t=( DT.formatters.statusClassFormatter(state) ))==null?'':__t)+
'">'+
((__t=(state))==null?'':__t)+
'</div>\n</div>\n\n';
 if (state === 'RUNNING') { 
__p+='\n    \n    <div class="right-top overview-item">\n        ';
 if (state === "RUNNING") { 
__p+='\n        <span class="key">As of:</span>\n        <span class="value">'+
((__t=(as_of))==null?'':__t)+
'</span>\n        ';
} else {
__p+='\n        <span class="key">Ended at</span>\n        <span class="value">'+
((__t=(as_of))==null?'':__t)+
'</span>\n        ';
}
__p+='\n    </div>\n    \n    <!-- lowest window id -->\n    <div class="overview-item currentWindowId">\n        <div class="key">last window id</div>\n        <div class="value">'+
((__t=(currentWindowId_f))==null?'':__t)+
'</div>\n    </div>\n    \n    <!-- recovery window id -->\n    <div class="overview-item currentWindowId">\n        <div class="key">recovery window id</div>\n        <div class="value">'+
((__t=(recoveryWindowId_f))==null?'':__t)+
'</div>\n    </div>\n\n    <!-- processed/sec -->\n    <div class="overview-item">\n        <div class="key">'+
((__t=( DT.text('processed_per_sec') ))==null?'':__t)+
'</div>\n        <div class="value">'+
((__t=(stats.tuplesProcessedPSMA_f))==null?'':__t)+
'</div>\n    </div>\n    \n    <!-- emitted/sec -->\n    <div class="overview-item">\n        <div class="key">'+
((__t=( DT.text('emitted_per_sec') ))==null?'':__t)+
'</div>\n        <div class="value">'+
((__t=(stats.tuplesEmittedPSMA_f))==null?'':__t)+
'</div>\n    </div>\n\n    <!-- total processed -->\n    <div class="overview-item">\n        <div class="key">'+
((__t=( DT.text('processed_total') ))==null?'':__t)+
'</div>\n        <div class="value">'+
((__t=(stats.totalTuplesProcessed_f))==null?'':__t)+
'</div>\n    </div>\n    \n    <!-- total emitted -->\n    <div class="overview-item">\n        <div class="key">'+
((__t=( DT.text('emitted_total') ))==null?'':__t)+
'</div>\n        <div class="value">'+
((__t=(stats.totalTuplesEmitted_f))==null?'':__t)+
'</div>\n    </div>\n\n    <!-- no. of operators -->\n    <div class="overview-item">\n        <div class="key">operators</div>\n        <div class="value">'+
((__t=(stats.numOperators))==null?'':__t)+
'</div>\n    </div>\n\n    <!-- no. of containers -->\n    <div class="overview-item">\n        <div class="key">planned/alloc. containers</div>\n        <div class="value">'+
((__t=(stats.plannedContainers))==null?'':__t)+
' / '+
((__t=(stats.allocatedContainers))==null?'':__t)+
' ('+
((__t=( totalAllocatedMemory ))==null?'':__t)+
' GB)</div>\n    </div>\n    \n    <!-- app-wide latency -->\n    <div class="overview-item">\n        <div class="key">latency (ms)</div>\n        <div class="value">'+
((__t=(stats.latency))==null?'':__t)+
'</div>\n    </div>\n';
 } 
__p+='\n\n<!-- up since -->\n<div class="overview-item">\n    <div class="key">up for</div>\n    <div class="value">'+
((__t=(up_for))==null?'':__t)+
'</div>\n</div>';
}
return __p;
},"/app/lib/widgets/JarAppActionsWidget/JarAppActionsWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="btn-group">\n\t<button class="launchApp btn btn-success"><i class="icon-play icon-white"></i> '+
((__t=( DT.text('launch app') ))==null?'':__t)+
'</button>\n\t<button class="btn btn-success dropdown-toggle" data-toggle="dropdown"><span class="caret"></span></button>\n\t<ul class="dropdown-menu">\n\t\t<li><a href="#" class="launchAppProps">launch with properties&hellip;</a></li>\n\t</ul>\n</div>';
}
return __p;
},"/app/lib/widgets/JarAppInfoWidget/JarAppInfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="info-item">\n\t<span class="key">'+
((__t=( DT.text('App Name') ))==null?'':__t)+
'</span>\n\t<span class="value">'+
((__t=( name ))==null?'':__t)+
'</span>\n</div>\n<div class="info-item">\n\t<span class="key">'+
((__t=( DT.text('Jar File') ))==null?'':__t)+
'</span>\n\t<span class="value">'+
((__t=( fileName ))==null?'':__t)+
'</span>\n</div>';
}
return __p;
},"/app/lib/widgets/JarAppsWidget/JarAppsPalette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<span class="select-status"><strong>'+
((__t=(selected.length))==null?'':__t)+
'</strong> '+
((__t=( DT.text('row(s) selected') ))==null?'':__t)+
'</span>\n<div class="palette-actions">\n\n\t';
 if (selected.length === 0) { 
__p+='\n\t\t<button class="btn" disabled>'+
((__t=( DT.text('select an item') ))==null?'':__t)+
'</button>\n\t';
 } else if (selected.length === 1) {
__p+='\n\t\t<button class="inspectItem btn btn-primary"><i class="icon-eye-open icon-white"></i> '+
((__t=( DT.text('inspect') ))==null?'':__t)+
'</button>\n\t\t<div class="btn-group">\n\t\t\t<button class="launchApp btn btn-success"><i class="icon-play icon-white"></i> '+
((__t=( DT.text('launch app') ))==null?'':__t)+
'</button>\n\t\t\t<button class="btn btn-success dropdown-toggle" data-toggle="dropdown"><span class="caret"></span></button>\n\t\t\t<ul class="dropdown-menu">\n\t\t\t\t<li><a href="#" class="launchAppProps">launch with properties&hellip;</a></li>\n\t\t\t</ul>\n\t\t</div>\n\t';
 } else { 
__p+='\n\t\t<button class="launchApp btn btn-success"><i class="icon-play icon-white"></i> '+
((__t=( DT.text('launch selected apps') ))==null?'':__t)+
'</button>\n\t';
 } 
__p+='\n    \n</div>';
}
return __p;
},"/app/lib/widgets/JarAppsWidget/JarAppsWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
}
return __p;
},"/app/lib/widgets/JarListWidget/JarListPalette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<span class="select-status"><strong>'+
((__t=(selected.length))==null?'':__t)+
'</strong> '+
((__t=( DT.text('row(s) selected') ))==null?'':__t)+
'</span>\n<div class="palette-actions">\n\t<button class="btn btn-info refreshList"><i class="icon-white icon-refresh icon-white"></i> '+
((__t=( DT.text('refresh list') ))==null?'':__t)+
' </button>\n\t';
 if (selected.length === 0) { 
__p+='\n\t\t<button class="btn" disabled>'+
((__t=( DT.text('select an item') ))==null?'':__t)+
'</button>\n\t';
 } else if (selected.length === 1) {
__p+='\n\t\t<button class="inspectItem btn btn-primary"><i class="icon-eye-open icon-white"></i> '+
((__t=( DT.text('inspect') ))==null?'':__t)+
'</button>\n\t\t<button class="removeJar btn btn-danger"><i class="icon-trash icon-white"></i> '+
((__t=( DT.text('remove file') ))==null?'':__t)+
'</button>\n\t';
 } else { 
__p+='\n\t\t<button class="removeJars btn btn-danger"><i class="icon-trash icon-white"></i> '+
((__t=( DT.text('remove selected files') ))==null?'':__t)+
'</button>\n\t';
 } 
__p+='\n    \n</div>';
}
return __p;
},"/app/lib/widgets/JarListWidget/JarListWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="uploader-target"></div><div class="palette-target"></div><div class="table-target"></div>';
}
return __p;
},"/app/lib/widgets/JarListWidget/JarUploadView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form enctype="multipart/form-data" class="jar_upload_form">\n    <input type="file" class="jar_upload" name="jar_upload" accept=".jar" />\n    <div class="well jar_upload_target">\n        ';
 if (name.length) { 
__p+='\n            <div class="drop-message"> <span class="jar_upload_filename">'+
((__t=( name ))==null?'':__t)+
'</span> <span class="jar_upload_filesize">('+
((__t=( Math.floor(size/1024) ))==null?'':__t)+
' KB)</span></div>\n            <div class="upload-buttons">\n                <button class="upload_jar_btn btn btn-success">upload</button>\n                <button class="cancel_jar_btn btn btn-danger">cancel</button>\n            </div>\n            <div class="progress progress-striped active">\n                <div class="jar_upload_progress bar" style="width: 0%;"></div>\n            </div>\n        ';
 } else { 
__p+='\n            <div class="drop-message">drag and drop jar file here</div>\n            <span class="click-message">(click to choose file)</span>\n        ';
 } 
__p+='\n    </div>\n</form>';
}
return __p;
},"/app/lib/widgets/LogicalDagWidget/LogicalDagWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="app-dag logical-dag">\n    <div class="form-inline">\n        <label class="metric-select-label">Top:</label>\n        <select class="form-control metric-select">\n            ';
 _.each(metrics, function (metric, index) { 
__p+='\n                <option value="'+
((__t=( metric.value ))==null?'':__t)+
'">'+
((__t=( metric.label ))==null?'':__t)+
'</option>\n            ';
 }) 
__p+='\n        </select>\n        <a href="#" class="btn metric-prev"><i class="icon-chevron-left"></i></a>\n        <a href="#" class="btn metric-next"><i class="icon-chevron-right"></i></a>\n\n        <label class="metric2-select-label">Bottom:</label>\n        <select class="form-control metric2-select">\n            ';
 _.each(metrics, function (metric, index) { 
__p+='\n            <option value="'+
((__t=( metric.value ))==null?'':__t)+
'">'+
((__t=( metric.label ))==null?'':__t)+
'</option>\n            ';
 }) 
__p+='\n        </select>\n        <a href="#" class="btn metric-prev2"><i class="icon-chevron-left"></i></a>\n        <a href="#" class="btn metric-next2"><i class="icon-chevron-right"></i></a>\n    </div>\n    <svg height="80">\n        <filter id="f1" x="0" y="0" height="150%">\n            <feOffset result="offOut" in="SourceGraphic" dx="5" dy="5" />\n            <feBlend in="SourceGraphic" in2="offOut" mode="normal" />\n        </filter>\n        <g transform="translate(20, 30)"/>\n    </svg>\n</div>\n';
}
return __p;
},"/app/lib/widgets/LogicalOpListWidget/LogicalOpListWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
}
return __p;
},"/app/lib/widgets/OpActionWidget/OpActionWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 if (recordingStartTime && recordingStartTime !== "-1") { 
__p+='\n    <button class="btn stopRecording"><i class="icon-rec-on"></i> stop recording</button>\n';
 } else { 
__p+='\n    <button class="btn startRecording"><i class="icon-rec-off"></i> start recording</button>\n';
 } 
__p+='';
}
return __p;
},"/app/lib/widgets/OpMetricsWidget/Ctrl.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form class="form-horizontal well">\n    <h4>Controls</h4>\n    <label for="toggle_multi_y" class="checkbox">\n        <input type="checkbox" class="toggle_multi_y" name="toggle_multi_y" ';
 if (multi_y) { print('checked ')} 
__p+='/> Multiple Y-Axes\n    </label>\n    \n    ';
 _.each( plots, function(plot) { 
__p+='\n        <label class="checkbox">\n            <input type="checkbox" data-key="'+
((__t=( plot.key ))==null?'':__t)+
'" />\n            <span class="plot-colorblock" style="background-color:'+
((__t=( plot.color ))==null?'':__t)+
'"></span>\n            '+
((__t=( plot.label ))==null?'':__t)+
'\n        </label>\n    ';
 }) 
__p+='\n    \n</form>';
}
return __p;
},"/app/lib/widgets/PerfMetricsWidget/Ctrl.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form class="form-horizontal">\n    <h4>Display Options</h4>\n    \n    <div class="input-append input-prepend" title="This is the range of the graph, in seconds">\n        <span class="add-on">last</span>\n        <input type="text" name="metric_limit" class="metric_limit input-small" placeholder="limit" />\n        <span class="add-on">seconds</span>\n    </div>\n    \n    <h4>Series</h4>\n    ';
 _.each( plots, function(plot) { 
__p+='\n        <label class="checkbox">\n            <input type="checkbox" data-key="'+
((__t=( plot.key ))==null?'':__t)+
'" />\n            <span class="plot-colorblock" style="background-color:'+
((__t=( plot.color ))==null?'':__t)+
'"></span>\n            '+
((__t=( plot.label ))==null?'':__t)+
'\n        </label>\n    ';
 }) 
__p+='\n    \n</form>';
}
return __p;
},"/app/lib/widgets/PerfMetricsWidget/PerfMetricsWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="perfmetrics-chart-ctrl chart-ctrl"></div>\n<div class="perfmetrics-chart-ctnr"></div>';
}
return __p;
},"/app/lib/widgets/PhysOpInfoWidget/PhysOpInfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="info-item">\n    <span class="key">Operator Name:</span>\n    <span class="value">';
 name ? print(name) : print('loading') 
__p+='</span>\n</div>\n\n<div class="info-item">\n    <span class="key">Operator ID:</span>\n    <span class="value">'+
((__t=(id))==null?'':__t)+
'</span>\n</div>';
}
return __p;
},"/app/lib/widgets/PhysOpListWidget/PhysOpListPalette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<span class="select-status"><strong>'+
((__t=(length))==null?'':__t)+
'</strong> row(s) selected</span>\n<div class="palette-actions">\n\n    ';
 if (none) { 
__p+='\n    <button disabled class="btn">no operator(s) selected</button>\n    ';
 } else if (one) {
__p+='\n        <button class="inspectOperator btn btn-primary">inspect</button>\n        ';
 if (isRecording) { 
__p+='\n            <button class="stopOpRecording btn"><i class="icon-rec-on"></i> stop recording</button>\n        ';
 } else { 
__p+='\n            <button class="startOpRecording btn"><i class="icon-rec-off"></i> start recording</button>\n        ';
 } 
__p+='\n    ';
 } else {
__p+='\n        <button class="btn" disabled>no actions available</button>\n    ';
 } 
__p+='\n    \n</div>';
}
return __p;
},"/app/lib/widgets/PhysOpOverviewWidget/PhysOpOverviewWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="overview-item">\n    <div class="key">'+
((__t=( DT.text('processed_per_sec') ))==null?'':__t)+
'</div>\n    <div class="value">'+
((__t=(tuplesProcessedPSMA_f))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">'+
((__t=( DT.text('processed_total') ))==null?'':__t)+
'</div>\n    <div class="value">'+
((__t=(totalTuplesProcessed_f))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">'+
((__t=( DT.text('emitted_per_sec') ))==null?'':__t)+
'</div>\n    <div class="value">'+
((__t=(tuplesEmittedPSMA_f))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">'+
((__t=( DT.text('emitted_total') ))==null?'':__t)+
'</div>\n    <div class="value">'+
((__t=(totalTuplesEmitted_f))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Status</div>\n    <div class="value '+
((__t=( DT.formatters.statusClassFormatter(status) ))==null?'':__t)+
'">'+
((__t=(status))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Container ID</div>\n    <div class="value">'+
((__t=(containerLink))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Host</div>\n    <div class="value">'+
((__t=(host))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Current Window</div>\n    <div class="value">'+
((__t=(currentWindowId_f))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Recovery Window</div>\n    <div class="value">'+
((__t=(recoveryWindowId_f))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n    <div class="key">Latency (ms)</div>\n    <div class="value">'+
((__t=( latencyMA ))==null?'':__t)+
'</div>\n</div>\n\n<div class="overview-item">\n\t<div class="key">CPU %</div>\n\t<div class="value">'+
((__t=( cpuPercentageMA ))==null?'':__t)+
'</div>\n</div>\n\n<!-- <div class="overview-item">\n    <div class="key">Failure Count</div>\n    <div class="value">'+
((__t=(failureCount))==null?'':__t)+
'</div>\n</div> -->';
}
return __p;
},"/app/lib/widgets/PortInfoWidget/PortInfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="info-item">\n    <span class="key">Port Name:</span>\n    <span class="value">'+
((__t=( name ))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n    <span class="key">Type:</span>\n    <span class="value">'+
((__t=( type ))==null?'':__t)+
'</span>\n</div>';
}
return __p;
},"/app/lib/widgets/PortListWidget/PortListPalette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<span class="select-status"><strong>'+
((__t=( selected.length ))==null?'':__t)+
'</strong> row(s) selected</span>\n<div class="palette-actions">\n\n    ';
 if (!selected.length) { 
__p+='\n    <button disabled class="btn">no port(s) selected</button>\n    ';
 } else if (selected.length === 1) {
__p+='\n       \t  <button class="inspectItem btn btn-primary"><i class="icon-eye-open icon-white"></i> '+
((__t=( DT.text('inspect') ))==null?'':__t)+
'</button>\n        ';
 if (operator.recordingStartTime == '-1') { 
__p+='\n            <button class="startPortRecording btn"><i class="icon-rec-off"></i> start recording</button>\n        ';
 } else { 
__p+='\n            <button disabled class="btn">recording in progress</button>\n        ';
 }
__p+='\n    ';
 } else {
__p+='\n        <button class="btn" disabled>no actions available</button>\n    ';
 } 
__p+='\n    \n</div>';
}
return __p;
},"/app/lib/widgets/PortMetricsWidget/Ctrl.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form class="form-horizontal well">\n    <h4>Controls</h4>\n    <label for="toggle_multi_y" class="checkbox">\n        <input type="checkbox" class="toggle_multi_y" name="toggle_multi_y" ';
 if (multi_y) { print('checked ')} 
__p+='/> Multiple Y-Axes\n    </label>\n    \n    ';
 _.each( plots, function(plot) { 
__p+='\n        <label class="checkbox">\n            <input type="checkbox" data-key="'+
((__t=( plot.key ))==null?'':__t)+
'" />\n            <span class="plot-colorblock" style="background-color:'+
((__t=( plot.color ))==null?'':__t)+
'"></span>\n            '+
((__t=( plot.label ))==null?'':__t)+
'\n        </label>\n    ';
 }) 
__p+='\n    \n</form>';
}
return __p;
},"/app/lib/widgets/PortOverviewWidget/PortOverviewWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="page-overview">\n    <div class="overview-item">\n        <span class="key">'+
((__t=( DT.text('tuples_total') ))==null?'':__t)+
'</span>\n        <span class="value">'+
((__t=( totalTuples_f ))==null?'':__t)+
'</span>\n    </div>\n\n    <div class="overview-item">\n        <span class="key">'+
((__t=( DT.text('tuples_per_sec') ))==null?'':__t)+
'</span>\n        <span class="value">'+
((__t=( tuplesPSMA_f ))==null?'':__t)+
'</span>\n    </div>\n\n    <div class="overview-item">\n        <span class="key">Buffer Server Bytes/sec:</span>\n        <span class="value">'+
((__t=( bufferServerBytesPSMA_f ))==null?'':__t)+
'</span>\n    </div>\n\n    <div class="widget-resize"></div>\n</div>\n';
}
return __p;
},"/app/lib/widgets/RecListWidget/Palette.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="palette-actions">\n\n    ';
 if (none) { 
__p+='\n    <button disabled class="btn">no recording(s) selected</button>\n    ';
 } else if (one) {
__p+='\n        <button class="inspectRecording btn btn-primary">view tuples</button>\n        ';
 if (!ended) { 
__p+='\n            <button class="stopOpRecording btn" data-appid="'+
((__t=( appId ))==null?'':__t)+
'" data-operatorid="'+
((__t=( operatorId ))==null?'':__t)+
'"><i class="icon-rec-on"></i> stop recording</button>\n        ';
 } 
__p+='\n    ';
 } else {
__p+='\n        <button class="btn" disabled>no actions available</button>\n    ';
 } 
__p+='\n    \n    <button class="btn btn-info refreshRecordings"><i class="icon-refresh icon-white"></i> refresh list</button>\n    \n</div>';
}
return __p;
},"/app/lib/widgets/StreamInfoWidget/StreamInfoWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="info-item">\n    <span class="key">Stream Name:</span>\n    <span class="value">'+
((__t=(model.name))==null?'':__t)+
'</span>\n</div>\n\n<div class="info-item">\n    <span class="key">Locality:</span>\n    <span class="value">';
 model.locality ? print(model.locality) : print('NOT ASSIGNED') 
__p+='</span>\n</div>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/TupleViewerWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="viewer-container"></div>\n<div class="widget-resize"></div>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/BodyView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div style="max-height: '+
((__t=( scrollerHeight ))==null?'':__t)+
'px;" class="console"></div>\n<div style="max-height: '+
((__t=( scrollerHeight ))==null?'':__t)+
'px;" class="scroller"></div>\n<div style="height: '+
((__t=( scrollerHeight ))==null?'':__t)+
'px;" class="list"></div>\n<div style="max-height: '+
((__t=( scrollerHeight ))==null?'':__t)+
'px;" class="dataview"></div>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/ConsoleView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="item window">\n    <div class="controls">\n        <button type="button" class="prev"><i class="icon-chevron-up"></i></button>\n        <button type="button" class="next"><i class="icon-chevron-down"></i></button>\n    </div>\n    <div class="display">\n        <div class="label">Window ID</div>\n        <div class="value currentWindowId" title="'+
((__t=( currentWindowId ))==null?'':__t)+
'">'+
((__t=( currentWindowIdShort ))==null?'':__t)+
'</div>\n    </div>\n</div>\n<div class="item tuple">\n    <div class="controls">\n        <button type="button" class="prev"><i class="icon-chevron-up"></i></button>\n        <button type="button" class="next"><i class="icon-chevron-down"></i></button>\n    </div>\n    <div class="display">\n        <div class="label">Tuple<br>Offset</div>\n        <div class="value currentTupleIndex">'+
((__t=( currentTupleIndex ))==null?'':__t)+
'</div>\n    </div>\n</div>\n<div class="item display-options">\n    <div class="display">\n        <div class="label">Show Ports:</div>\n        <form action="" class="ports-frm"></form>\n    </div>\n</div>\n';
 if (!ended) { 
__p+='\n<div class="item">\n    <div class="display">\n        <button type="button" class="btn btn-primary syncRecording"><i class="icon-refresh icon-white"></i> refresh recording</button>\n        <div class="sync-rec-note">This recording has not yet ended, so pressing this button will update the tuple count and visible tuples in this widget.</div>\n    </div>\n</div>\n';
 } 
__p+='';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/HeaderView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='';
 if (startTime) { 
__p+='\n\n<div class="info">\n    <span class="title-key">start time</span>\n    <span class="title-value">'+
((__t=( startTime ))==null?'':__t)+
'</span>\n</div>\n\n<div class="info">\n    <span class="title-key">no. of tuples</span>\n    <span class="title-value">'+
((__t=( totalTuples ))==null?'':__t)+
'</span>\n</div>\n\n';
 } else { print('<div class="info"><span class="title-key">status</span> <span class="title-value">' + loadedStatus + '</span>') } 
__p+='';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/ListView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="inner">\n    ';
 if (!appId || !operatorId || !startTime) { 
__p+='\n    <div class="tuple loading" style="padding:'+
((__t=( tuplePaddingTB ))==null?'':__t)+
'px 0; margin-bottom:'+
((__t=( tupleMarginBtm ))==null?'':__t)+
'px"><span class="type"></span> <span>no recording found</span></div>\n    ';
 } else if (totalTuples == '0') { 
__p+='\n    <div class="tuple loading" style="padding:'+
((__t=( tuplePaddingTB ))==null?'':__t)+
'px 0; margin-bottom:'+
((__t=( tupleMarginBtm ))==null?'':__t)+
'px"><span class="type"></span> <span>no tuples in this recording</span></div>\n    ';
 } else if (currentTotal == '0') { 
__p+='\n    <div class="tuple loading" style="padding:'+
((__t=( tuplePaddingTB ))==null?'':__t)+
'px 0; margin-bottom:'+
((__t=( tupleMarginBtm ))==null?'':__t)+
'px"><span class="type"></span> <span>no tuples for selected ports</span></div>\n    ';
 }
__p+='\n</div>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/PortTemplate.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="port">\n    <input type="checkbox" name="port-'+
((__t=( id ))==null?'':__t)+
'" id="port-'+
((__t=( id ))==null?'':__t)+
'" value="'+
((__t=( id ))==null?'':__t)+
'" class="port-checkbox" ';
 if (selected) { print('checked="checked"') } 
__p+=' />\n    <label for="port-'+
((__t=( id ))==null?'':__t)+
'">'+
((__t=( name ))==null?'':__t)+
' ('+
((__t=( streamName ))==null?'':__t)+
')</label>\n</div>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/ScrollerView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<canvas width="'+
((__t=( scrollerWidth ))==null?'':__t)+
'px" height="'+
((__t=( scrollerHeight ))==null?'':__t)+
'px"></canvas>\n<div style="width: '+
((__t=( scrollerWidth ))==null?'':__t)+
'px; height: '+
((__t=( scrollViewportHeight ))==null?'':__t)+
'px; top: '+
((__t=( scrollViewportTop ))==null?'':__t)+
'px;" class="viewport"></div>\n<div style="width: '+
((__t=( scrollerWidth ))==null?'':__t)+
'px; height: '+
((__t=( scrollerHeight ))==null?'':__t)+
'px;" class="interaction"></div>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/TupleView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<a href="#" class="tuple '+
((__t=( type ))==null?'':__t)+
' '+
((__t=( selected ))==null?'':__t)+
'" style="height: '+
((__t=( height ))==null?'':__t)+
'px; padding: '+
((__t=( padding ))==null?'':__t)+
'px 1.5%; margin-bottom: '+
((__t=( marginBottom ))==null?'':__t)+
'px;">\n    <span class="index">( '+
((__t=( idx ))==null?'':__t)+
' )</span>\n    <span class="type">'+
((__t=( type ))==null?'':__t)+
'</span>\n    <span class="port">'+
((__t=( port ))==null?'':__t)+
'</span>\n    <span class="view-tuple">click to view</span>\n</a>';
}
return __p;
},"/app/lib/widgets/TupleViewerWidget/lib/TvView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="viewer-header"></div>\n<div class="viewer-body" tabindex="1"></div>';
}
return __p;
},"/app/lib/widgets/OpChartWidget/OpChartWidget.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="title">\n    <span class="inner">Operator Chart</span>\n</div>\n<div class="ctrl-target chart-ctrl"></div>\n<div class="chart-trg-ctnr">\n    <div class="chart-target"></div>\n</div>\n<div class="widget-resize"></div>';
}
return __p;
},"/app/lib/widgets/OpChartWidget/opchart_setup.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="title">\n    <span class="inner">Recording Chart</span>\n</div>\n<form class="opchart-select-form form-inline">\n    <select class="opchart-options">\n        <option value="">Select a recording to chart</option>\n        ';
 _.each(options, function(option) { 
__p+='\n        <option value="'+
((__t=( option.recordingName ))==null?'':__t)+
'">('+
((__t=( option.properties.chartType ))==null?'':__t)+
') Operator '+
((__t=( option.operatorId ))==null?'':__t)+
', Started '+
((__t=( option.startTimeFormatted ))==null?'':__t)+
' ago, name: '+
((__t=( option.recordingName ))==null?'':__t)+
'</option>\n        ';
 }) 
__p+='\n    </select>\n    <button class="btn btn-primary chartRecording">chart</button>\n</form>';
}
return __p;
},"/app/lib/widgets/OpChartWidget/lib/Line/LineControl.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<form action="#" class="form-horizontal well">\n    \n    <h4 title="These are the auto-detected series that can be plotted from your charting operator\'s output.">Series</h4>\n    \n    ';
 _.each( plots, function(plot) { 
__p+='\n        <label class="checkbox">\n            <input type="checkbox" data-key="'+
((__t=( plot.key ))==null?'':__t)+
'" />\n            <span class="plot-colorblock" style="background-color:'+
((__t=( plot.color ))==null?'':__t)+
'"></span>\n            '+
((__t=( plot.label ))==null?'':__t)+
'\n        </label>\n    ';
 }) 
__p+='\n    \n    <h4>Data Settings</h4>\n    \n    <label for="toggle_tail" class="checkbox" title="As new data is received, the chart position will be updated to display it. Only applies to live recordings.">\n        <input type="checkbox" name="toggle_tail" class="toggle_tail" /> Tail Incoming Data\n    </label>\n    \n    <div class="input-append" title="This is the location of the recording that the chart is currently displaying. This is not editable when \'Tail Incoming Data\' is checked.">\n        <input type="text" name="rec_offset" class="rec_offset input-small" placeholder="offset" />\n        <span class="add-on">Offset</span>\n    </div>\n    \n    <div class="input-append" title="This is the data point range from the offset that should be displayed in the viewport. Above a certain amount, the tuple set will need to be downsampled for rendering.">\n        <input type="text" name="rec_limit" class="rec_limit input-small" placeholder="limit" />\n        <span class="add-on">Limit</span>\n    </div>\n    \n    <h4>Display Settings</h4>\n    \n    <label for="toggle_multi_y" class="checkbox" title="When this is checked, each series will be plotted on its own y-axis. When it is not, all series are plotted on a single axis.">\n        <input type="checkbox" name="toggle_multi_y" class="toggle_multi_y" /> Multiple Y-Axes\n    </label>\n    \n    <label for="toggle_render_points" class="checkbox" title="This toggles whether or not points on the lines should be rendered.">\n        <input type="checkbox" class="toggle_render_points" name="toggle_render_points" /> Render Points\n    </label>\n    \n    <label for="x_format_select" title="The options in this dropdown are predefined format functions for x-axis values. Select one and the chart will update automatically.">X-Axis Format</label>\n    \n    <select class="x_format_select" name="x_format_select" title="The options in this dropdown are predefined format functions for x-axis values. Select one and the chart will update automatically.">\n        <option value="">-- no format --</option>\n        ';
 _.each( x_formats, function(fn, key) { 
__p+='\n            <option value="'+
((__t=( key ))==null?'':__t)+
'">'+
((__t=( key ))==null?'':__t)+
'</option>\n        ';
 }) 
__p+='\n    </select>\n    \n</form>\n\n';
}
return __p;
},"/app/lib/widgets/OpChartWidget/lib/Line/LineOpChartView.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<div class="opchart-viewport charted-ctnr"></div>\n<div class="opchart-overview"></div>\n<div class="opchart-controls"></div>';
}
return __p;
},"/app/lib/widgets/OpChartWidget/lib/Line/Overview.html": function(obj){
var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};
with(obj||{}){
__p+='<canvas height="'+
((__t=( scroller_height ))==null?'':__t)+
'" width="'+
((__t=( scroller_width ))==null?'':__t)+
'"></canvas>\n<div class="interaction" style="width: '+
((__t=( scroller_width ))==null?'':__t)+
'px; height: '+
((__t=( scroller_height ))==null?'':__t)+
'px;">\n    <div class="viewport" style="left: '+
((__t=( viewport_left ))==null?'':__t)+
'px; width: '+
((__t=( viewport_width ))==null?'':__t)+
'px; height: '+
((__t=( scroller_height ))==null?'':__t)+
'px;">\n        <!-- <div class="left-handle vp-handle">\n            <div class="handle-grip"></div>\n        </div>\n        <div class="right-handle vp-handle">\n            <div class="handle-grip"></div>\n        </div> -->\n        <div class="ticker"></div>\n    </div>    \n</div>';
}
return __p;
}};