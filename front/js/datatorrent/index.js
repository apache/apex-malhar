/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
var datatorrent = {
    // Internal settings
    settings: require('./settings'),
    // Main library components
    lib: {
        AbstractJarFileCollection    : require('./AbstractJarFileCollection'),
        AbstractJarFileModel         : require('./AbstractJarFileModel'),
        AlertCollection              : require('./AlertCollection'),
        AlertModel                   : require('./AlertModel'),
        AlertParamsModel             : require('./AlertParamsModel'),
        AlertTemplateCollection      : require('./AlertTemplateCollection'),
        AlertTemplateModel           : require('./AlertTemplateModel'),
        App                          : require('./App'),
        ApplicationCollection        : require('./ApplicationCollection'),
        ApplicationModel             : require('./ApplicationModel'),
        ApplicationSubCollection     : require('./ApplicationSubCollection'),
        AppJarFileModel              : require('./AppJarFileModel'),
        AppJarFileCollection         : require('./AppJarFileCollection'),
        BaseCollection               : require('./BaseCollection'),
        BaseModel                    : require('./BaseModel'),
        BasePageView                 : require('./BasePageView'),
        Bbindings                    : require('./Bbindings'),
        Breadcrumbs                  : require('./Breadcrumbs'),
        ContainerCollection          : require('./ContainerCollection'),
        ContainerModel               : require('./ContainerModel'),
        ClusterMetricsModel          : require('./ClusterMetricsModel'),
        ConfigPropertyModel          : require('./ConfigPropertyModel'),
        ConfigPropertyCollection     : require('./ConfigPropertyCollection'),
        ConfigIssueModel             : require('./ConfigIssueModel'),
        ConfigIssueCollection        : require('./ConfigIssueCollection'),
        DagOperatorView              : require('./DagOperatorView'),
        DagView                      : require('./DagView'),
        DashCollection               : require('./DashCollection'),
        DashMgrView                  : require('./DashMgrView'),
        DashModel                    : require('./DashModel'),
        DataSource                   : require('./DataSource'),
        DepJarFileModel              : require('./DepJarFileModel'),
        DepJarFileCollection         : require('./DepJarFileCollection'),
        HeaderView                   : require('./HeaderView'),
        JarAppCollection             : require('./JarAppCollection'),
        JarAppModel                  : require('./JarAppModel'),
        ListPalette                  : require('./widgets/ListWidget/ListPalette'),
        LiveChart                    : require('./livechart/LiveChart'),
        LicenseModal                 : require('./LicenseModalView'),
        LicenseModel                 : require('./LicenseModel'),
        LogicalOperatorModel         : require('./LogicalOperatorModel'),
        LogicalOperatorCollection    : require('./LogicalOperatorCollection'),
        ModalView                    : require('./ModalView'),
        NavModel                     : require('./NavModel'),
        Notifier                     : require('./Notifier'),
        OpPropertiesModel            : require('./OpPropertiesModel'),
        OperatorClassCollection      : require('./OperatorClassCollection'),
        OperatorClassModel           : require('./OperatorClassModel'),
        OperatorCollection           : require('./OperatorCollection'),
        OperatorModel                : require('./OperatorModel'),
        PageLoaderView               : require('./PageLoaderView'),
        PortCollection               : require('./PortCollection'),
        PortModel                    : require('./PortModel'),
        RecordingCollection          : require('./RecordingCollection'),
        RecordingModel               : require('./RecordingModel'),
        RestartModal                 : require('./RestartModalView'),
        StreamCollection             : require('./StreamCollection'),
        StreamModel                  : require('./StreamModel'),
        StreamPortCollection         : require('./StreamPortCollection'),
        StreamPortModel              : require('./StreamPortModel'),
        Tabled                       : require('./tabled'),
        TupleCollection              : require('./TupleCollection'),
        TupleModel                   : require('./TupleModel'),
        UploadFileCollection         : require('./UploadFileCollection'),
        UploadFileModel              : require('./UploadFileModel'),
        UploadFilesView              : require('./UploadFilesView'),
        UserModel                    : require('./UserModel'),
        WidgetClassCollection        : require('./WidgetClassCollection'),
        WidgetClassModel             : require('./WidgetClassModel'),
        WidgetCtrlView               : require('./WidgetCtrlView'),
        WidgetDefCollection          : require('./WidgetDefCollection'),
        WidgetDefModel               : require('./WidgetDefModel'),
        WidgetHeaderView             : require('./WidgetHeaderView'),
        WidgetSettingsView           : require('./WidgetSettingsView'),
        WidgetView                   : require('./WidgetView'),
        WindowId                     : require('./WindowId')
    },
    // Widgets
    widgets: {
        Widget: require('./WidgetView'),
        ActionWidget: require('./widgets/ActionWidget'),
        InfoWidget: require('./widgets/InfoWidget'),
        OverviewWidget: require('./widgets/OverviewWidget'),
        ListWidget: require('./widgets/ListWidget'),
        TopNWidget: require('./widgets/TopNWidget'),
        AngularWidget: require('./widgets/AngularWidget')
    },

    // Templates
    templates: require('./templates'),
    
    // Formatters
    formatters: require('./formatters'),
    
    // Methods for extending
    registerWidget: function(name, widget) {
        if (this.widgets.hasOwnProperty(name)) {
            throw new Error('A widget by this name is already registered.');
        }
        this.widgets[name] = widget;
    },
    
    text: require('./text')
};
exports = module.exports = datatorrent;