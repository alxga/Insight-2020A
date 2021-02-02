function FCPlot(divSelector) {
  /* Plot builder object

    Uses functions exposed by D3 and D3FC javascript libraries

    Args:
      divSelector - identifier (starting with #) of a DIV element to contain
        the plot
  */
  var _this = this;

  _this.divSelector = divSelector;

  _this.d3ParseTime = d3.timeParse('%Y-%m-%d %H:%M:%S');

  // Default axes limits
  _this.xLims = [new Date(2020, 0, 1), new Date(2020, 1, 1)]
  _this.yLims = [-500, 1500]

  // Day of Week mode that was used to build the chart;
  // undefined if no chart has been built yet
  _this._curDayOfWeekMode = undefined;


  _this.initXLimsFromData = function(data) {
    /* Initializes the horizontal axis limits based on a dataset

      Args:
        Array of data entries
    */
    var mnDt = moment(new Date(3000, 0, 1));
    var mxDt = moment(new Date(1000, 0, 1));

    for (var i = 0; i < data.length; i++) {
      dt = moment(data[i].dt, 'YYYY-MM-DD HH:mm:ss');
      if (dt.isBefore(mnDt))
        mnDt = dt;
      if (dt.isAfter(mxDt))
        mxDt = dt;
    }

    var daysDiff = mxDt.diff(mnDt, 'days')
    if (daysDiff > 100) {
      mnDt = mxDt.clone().subtract(100, 'days');
    }

    mnDt = mnDt.set({'hour': 0, 'minute': 0, 'second': 0});

    mxDt.add(1, 'days')
    mxDt.set({'hour': 0, 'minute': 0, 'second': 0});

    _this.xLims = [mnDt.toDate(), mxDt.toDate()];
  }


  _this.skipWeekdaysDiscontinuityProvider = function() {
    /* Provides an fc.discontinuityRange to skip weekends */
    
    mnDt = moment(new Date(_this.xLims[0]));
    mxDt = moment(new Date(_this.xLims[1]));

    mnDt.day(1);
    mxDt.day(8);

    ranges = [];
    while (mnDt < mxDt) {
      sat = mnDt.clone().day(6);
      rng = [mnDt.toDate(), sat];
      ranges.push(rng);
      mnDt.day(8);
    }
    return fc.discontinuityRange(...ranges)
  }


  _this.createPlot = function(dayOfWeekMode) {
    /* Creates or updates a plot */

   _this._curDayOfWeekMode = dayOfWeekMode;

    var yScale, xTickFilter, gridXFilter;
    if (_this._curDayOfWeekMode == '') {
      yScale = d3.scaleTime();
      xTickFilter = d3.timeDay.filter((d) => d.getDay() === 1);
      gridXFilter = xTickFilter;
      gridXFilter = d3.timeDay.filter((d) => true);
    }
    else if (_this._curDayOfWeekMode == 'Weekdays') {
      yScale = fc.scaleDiscontinuous(d3.scaleTime())
        .discontinuityProvider(fc.discontinuitySkipWeekends());
      xTickFilter = d3.timeDay.filter((d) => (
        d.getDay() === 1));
      gridXFilter = d3.timeDay.filter((d) => (
        d.getDay() !== 0 && d.getDay() !== 6));
    }
    else if (_this._curDayOfWeekMode == 'Weekends') {
      yScale = fc.scaleDiscontinuous(d3.scaleTime())
        .discontinuityProvider(_this.skipWeekdaysDiscontinuityProvider());
      xTickFilter = d3.timeDay.filter((d) => (
        d.getDay() === 6));
      gridXFilter = d3.timeDay.filter((d) => (
        d.getDay() === 0 || d.getDay() === 6));
    }

    var gridlines = fc.annotationSvgGridline().xTicks(gridXFilter);

    const seriesLine = fc.seriesSvgLine()
      .crossValue(d => d.dt)
      .mainValue(d => d.value)
      .decorate(function(selection) {
        selection.enter()
          .attr('fill', 'none')
          .attr('stroke', 'black')
          .attr('stroke-width', 1.5);
        selection.exit().remove();
      });
    var seriesMarkers = fc.seriesSvgPoint()
      .crossValue(d => d.dt)
      .mainValue(d => d.value)
    seriesMarkers = seriesMarkers.size(20)
      .type(d3.symbolCircle)
      .decorate(function(selection) {
        selection.enter()
          .attr('class', 'point plot-marker')
        selection.exit().remove();
      });

    const multi = fc.seriesSvgMulti()
      .series([gridlines, seriesLine, seriesMarkers])
      .mapping((data, index, series) => {
        switch (series[index]) {
          case seriesMarkers:
            return data.filter(d => d.dt.getHours() == 0);
          default:
            return data;
        }
      });

    if (_this.xLims[0] > _this.xLims[1])
      effXLims = [new Date(2020, 0, 1), new Date(2020, 0, 1)];
    else {
      effXLims = _this.xLims;
    }
      
    
    _this.chart = fc.chartCartesian(yScale, d3.scaleLinear())
      // .xAxisHeight('3.5em')
      .yOrient('left')
      .xLabel('Date')
      .yLabel('Delay, seconds')
      .xDomain(effXLims)
      .yDomain(_this.yLims)
      .xTicks(xTickFilter)
      .xTickFormat(d3.timeFormat('%b-%d'))
      /*.xDecorate(function(selection) {
        selection.select('text')
          .style('text-anchor', 'start')
          .attr('transform', 'rotate(45 -10 10)');
      })*/;

    _this.chart.svgPlotArea(multi);
  }


  _this.updatePlot = function(data, dayOfWeekMode) {
    /* Updates the plot from a dataset

      Args:
        data: Array of data entries
        dayOfWeekMode: '', 'Weekdays', or 'Weekends'
    */

    if (_this._curDayOfWeekMode !== dayOfWeekMode)
    {
      // Rebuild the entire chart if the day of the week mode has changed
      _this.createPlot(dayOfWeekMode);
    }

    data.forEach(function(elem) {
      elem.dt = _this.d3ParseTime(elem.dt);
    });
    if (_this._curDayOfWeekMode == 'Weekdays')
      data = data.filter((x) => (x.dt.getDay() != 0 && x.dt.getDay() != 6));
    else if (_this._curDayOfWeekMode == 'Weekends')
      data = data.filter((x) => (x.dt.getDay() == 0 || x.dt.getDay() == 6));

    d3.select(_this.divSelector)
      .datum(data)
      .transition().duration(500)
      .call(_this.chart);
  }
}
