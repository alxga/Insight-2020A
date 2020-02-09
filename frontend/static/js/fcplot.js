const xExtent = fc.extentTime().accessors([d => d.dt])
const yExtent = fc.extentLinear().accessors([d => d.value])

function updatePlot(data) {
  var parseTime = d3.timeParse("%Y-%m-%d %H:%M:%S");
  data.forEach(function(elem) {
    elem.dt = parseTime(elem.dt);
  });

  //data = data.filter((x) => (x.dt.getDay() != 6 && x.dt.getDay() != 0));

  const xTickFilter = d3.timeDay.filter((d) => d.getDay() === 1);
  const skipWeekendScale = fc.scaleDiscontinuous(d3.scaleTime())
    .discontinuityProvider(fc.discontinuitySkipWeekends());
  const yScale = d3.scaleTime();

  var chart = fc.chartCartesian(yScale, d3.scaleLinear())
    .xAxisHeight('3.5em')
    .yOrient("left")
    .xLabel('Value')
    .yLabel('Delay, seconds')
    .xDomain(xExtent(data))
    .yDomain([-300, 1800])
    .xTicks(xTickFilter)
    .xDecorate(function(selection) {
      selection.select('text')
        .style('text-anchor', 'start')
        .attr('transform', 'rotate(45 -10 10)');
    })

  var gridlines = fc.annotationSvgGridline()
    .xTicks(xTickFilter);
  const seriesLine = fc.seriesSvgLine()
    .crossValue(d => d.dt)
    .mainValue(d => d.value)
    .decorate(function(selection) {
      selection.enter()
        .attr("fill", "none")
        .attr("stroke", "black")
        .attr("stroke-width", 1.5);
      selection.exit().remove();
    });
  var seriesMarkers = fc.seriesSvgPoint()
    .crossValue(d => d.dt)
    .mainValue(d => d.value)
  seriesMarkers = seriesMarkers.size(20)
    .type(d3.symbolCircle)
    .decorate(function(selection) {
      selection.enter()
        .attr("class", "point plot-marker")
      selection.exit().remove();
    });

  const multi = fc.seriesSvgMulti()
    .series([gridlines, seriesLine, seriesMarkers]);

  chart.svgPlotArea(multi);

  d3.select('#plot-div')
    .datum(data)
    .transition().duration(500)
    .call(chart);
}
