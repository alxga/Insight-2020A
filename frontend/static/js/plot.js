// set the dimensions and margins of the graph

var plotDiv = document.getElementById("plot-div");
var plotSvg = d3.select(plotDiv).append("svg")
  .attr("class", "embed-responsive-item");
var clWidth = plotDiv.clientWidth;
var clHeight = plotDiv.clientHeight;

var margin = {top: 10, right: 40, bottom: 30, left: 50},
    width = clWidth - margin.left - margin.right,
    height = clHeight - margin.top - margin.bottom;

// append the svg object to the body of the page
plotSvg = plotSvg
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
.append("g")
  .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

plotSvg.append('g').attr("id", "g-markers")
plotSvg.append('g').attr("id", "g-lines")

// X Axis
var xScale = d3.scaleTime()
  .domain([new Date(2020, 01, 15), new Date(2020, 02, 01)])
  .range([ 0, width ]);
var xAxis = d3.axisBottom(xScale)
plotSvg.append('g')
  .attr("class", "xaxis")
  .attr("transform", "translate(0," + height + ")")
  .call(xAxis);

// Y Axis
var yScale = d3.scaleLinear()
  .domain([-300, 1800])
  .range([height, 0]);
var yAxis = d3.axisLeft(yScale)
plotSvg.append('g')
  .attr("class", "yaxis")
  .call(yAxis);

// Function to Create/Update the Main Plot
function updatePlot(data) {
  data.forEach(function(elem) {
    elem.dt = d3.timeParse("%Y-%m-%d %H:%M:%S")(elem.dt);
  });

  xScale.domain(d3.extent(data, function(d) { return d.dt; }))
  plotSvg.select(".xaxis")
    .call(xAxis);

  // Add the main plotted line
  var path = plotSvg.select("#g-lines").selectAll("#plot-line")
    .data([data])  
  path.exit().remove();
  path.enter().append("path").attr("id", "plot-line")
  path
    .attr("fill", "none")
    .attr("stroke", "black")
    .attr("stroke-width", 1.5)
    .attr(
      "d",
      d3.line().curve(d3.curveBasis)
        .x(function(d) { return xScale(d.dt) })
        .y(function(d) { return yScale(d.value) })
    )

  // Add the markers
  var circle = plotSvg.select("#g-markers").selectAll("circle")
    .data(data);
  circle.exit().remove();
  circle.enter().append("circle")
    .attr("cx", function(d) { return xScale(d.dt) } )
    .attr("cy", function(d) { return yScale(d.value) } )
    .attr("r", 2)
  circle
    .attr("cx", function(d) { return xScale(d.dt) } )
    .attr("cy", function(d) { return yScale(d.value) } )
    .attr("r", 2)
    .attr("class", "myCircle")
    .attr("stroke", "#69b3a2")
    .attr("stroke-width", 2)
    .attr("fill", "white");
}
