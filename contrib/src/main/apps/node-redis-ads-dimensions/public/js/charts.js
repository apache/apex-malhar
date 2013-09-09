var chartOptions = { width: 600, height: 300, legend: 'none', pointSize: 0, lineWidth : 1 };

function drawCostChart(data) {
    drawChart(data, {
        title: 'Cost Chart',
        container: 'chart_div',
        column: 'Cost',
        fn: function(item  ) {
            return item.cost;
        }
    });
}

function drawRevenueChart(data) {
    drawChart(data, {
        title: 'Revenue Chart',
        container: 'chart1_div',
        column: 'Revenue',
        fn: function(item) {
            return item.revenue;
        }
    });
}

function drawClicksChart(data) {
    drawChart(data, {
        title: 'Clicks Chart',
        container: 'chart2_div',
        column: 'Clicks',
        fn: function(item) {
            return item.clicks;
        }
    });
}

function drawImpressionsChart(data) {
    drawChart(data, {
        title: 'Impressions Chart',
        container: 'chart3_div',
        column: 'Impressions',
        fn: function(item) {
            return item.impressions;
        }
    });
}

function drawCtrChart(data) {
    drawChart(data, {
        title: 'Ctr Chart',
        container: 'chart4_div',
        column: 'Ctr',
        fn: function(item) {
            return item.clicks / item.impressions * 100;
        }
    });
}

function drawMarginChart(data) {
    drawChart(data, {
        title: 'Margin Chart',
        container: 'chart5_div',
        column: 'Margin',
        fn: function(item) {
            return (item.cost - item.revenue) / item.revenue;
        }
    });
}

function drawChart(data, options) {
    var table = new google.visualization.DataTable();
    table.addColumn('datetime', 'Time');
    table.addColumn('number', options.column);
    table.addRows(data.length);

    var costChart = new google.visualization.ScatterChart(document.getElementById(options.container));
    var costView = new google.visualization.DataView(table);

    // Populate data table
    var fn = options.fn;
    for(var i=0; i < data.length; i++)
    {
        var item = data[i];
        table.setCell(i, 0, new Date(item.timestamp));
        table.setCell(i, 1, fn(item));
    }

    // Draw line chart
    var chartOpt = { title: options.title };
    costChart.draw(costView, jQuery.extend(chartOpt, chartOptions));
}