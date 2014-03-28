require('backbone').$ = $;

window.DT = require('../js/datatorrent');
require('../js/datatorrent/AlertModel.spec');
require('../js/datatorrent/AlertCollection.spec');
require('../js/datatorrent/ApplicationCollection.spec');
require('../js/datatorrent/ApplicationModel.spec');
require('../js/datatorrent/ApplicationSubCollection.spec');
require('../js/datatorrent/BaseUtil.spec');
require('../js/datatorrent/BaseModel.spec');
require('../js/datatorrent/BaseCollection.spec');
require('../js/datatorrent/BasePageView.spec');
require('../js/datatorrent/ContainerModel.spec');
require('../js/datatorrent/ClusterMetricsModel.spec');
require('../js/datatorrent/DataSource/DataSource.spec');
require('../js/datatorrent/formatters.spec');
require('../js/datatorrent/JarAppModel.spec');
require('../js/datatorrent/LogicalOperatorCollection.spec');
require('../js/datatorrent/livechart/test/model.test');
require('../js/datatorrent/widgets/ListWidget/ListPalette.spec');
require('../js/datatorrent/ModalView.spec');
require('../js/datatorrent/NavModel/NavModel.spec');
require('../js/datatorrent/OpPropertiesModel.spec');	
require('../js/datatorrent/OperatorModel.spec');
require('../js/datatorrent/PortModel.spec');
require('../js/datatorrent/RecordingModel.spec');
require('../js/datatorrent/tabled/test/suite.js');
require('../js/datatorrent/TupleCollection.spec');
require('../js/datatorrent/TupleModel.spec');
require('../js/datatorrent/WidgetDefCollection.spec');
require('../js/datatorrent/WidgetDefModel.spec');
require('../js/datatorrent/WidgetView.spec');
require('../js/datatorrent/WindowId.spec');
require('../js/datatorrent/ModeCollection.spec');
require('../js/datatorrent/RestartModalView/RestartModalView.spec');


// Page Tests
require('../js/datatorrent/PageLoaderView/PageLoaderView.spec');
require('../js/app/lib/pages/AppInstancePageView.spec');
require('../js/app/lib/pages/PortPageView.spec');


// Widget Tests
require('../js/datatorrent/widgets/OverviewWidget/OverviewWidget.spec');
require('../js/app/lib/widgets/PortInfoWidget/PortInfoWidget.spec');
require('../js/app/lib/widgets/PortOverviewWidget/PortOverviewWidget.spec');
require('../js/app/lib/widgets/PerfMetricsWidget/PerfMetricsWidget.spec');
require('../js/app/lib/widgets/LogicalDagWidget/MetricModel.spec');
require('../js/app/lib/widgets/AppListWidget/columns.spec');
require('../js/app/lib/widgets/PhysOpListWidget/PhysOpListWidget.spec');