document.currentScript = document.currentScript || (function() {
  var scripts = document.getElementsByTagName('script');
  return scripts[scripts.length - 1];
})();

Highcharts.chart(document.currentScript.getAttribute('container'), {
    chart: {
        type: 'area'
    },
    colors: ['#FFC300', '#EB6114', '#C70039', 'black'],
title: {
      text: 'Casos registrados' + _case_type + ' por dia'
},
subtitle: {
      text: 'Fuente: ECDC - ' + _dt_ecdc
},
xAxis: {
      categories: _fechas,
      crosshair: false
},
yAxis: {
      min: 0,
      title: {
  text: 'Casos'
      }
},
tooltip: {
      headerFormat: '<span style="font-size:10px">{point.key}</span><table>',
      pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
  '<td style="padding:0"><b>{point.y}</b></td></tr>',
      footerFormat: '</table>',
      shared: true,
      useHTML: true
},
plotOptions: {
      area: {
          pointPadding: 0.2,
          borderWidth: 0
          ,marker: {
                radius: 2
            }
        },
        line: {
        marker: {
            enabled: false,
                radius: 1
            }
        },
        series: {
            dataLabels: {
                enabled: false,
                align: 'right',
                color: '#444444',
                rotation: -0,
                y: -0
            },
            pointPadding: 0.1,
            groupPadding: 0
        }
},
series: _v_fechas
});