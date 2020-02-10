/* Main Javascript file for Frontend
*/

// Special routes dictionary, maps from representation in lists to 
// the API values
SPEC_ROUTES = {
  "": "",
  "Buses Only": "ALLBUSES",
  "Trains Only": "ALLTRAINS"
}
// Day of the week modes for the chart
const DOWVals = [
  "", "Weekdays", "Weekends"
]

// Object handling the autocompletion of the stops list
var stopsAutocomplete = new Autocomplete($("#ByStop")[0], []);

// Object to create and update the plot
var plotHandler = undefined;


function updateCombo(selector, choice, arr) {
  /* Updates a dropdown list to containt the given values

    Args:
      selector: the dropdown element selector
      choice: option that is to be set as chosen if any
      arr: list of values for the element
  */
  var combo = $(selector);
  combo.empty();
  $.each(arr,
         function (index, item) {
           if (choice == item)
             combo.append(
                  $('<option/>', {
                      value: item,
                      text: item,
                      selected: ""
                  }));
          else
            combo.append(
                  $('<option/>', {
                      value: item,
                      text: item
                  }));
         });
}


function fillRoutes() {
  /* Updates the routes dropdown list */

  var selector = "#ByRoute";
  $.ajax({url: "/mbta/api/routeids", success: function(data){
    routeIds = Object.keys(SPEC_ROUTES).concat(data.items);
    updateCombo(selector, "", routeIds);
  }});
}

function updateStopsList() {
  /* Updates the stops dropdown list */

  $("#ByStop").val('')

  routeId = $("#ByRoute").val();
  if (routeId in SPEC_ROUTES) {
    routeId = SPEC_ROUTES[routeId]
  }

  $.ajax({
    url: "/mbta/api/stopnames",
    data: {
      routeId: routeId
    },
    success: function(result){
      stopsAutocomplete.values = result.items;
    }
  });
  updateDataAndPlot();
}

function fillDOW() {
  /* Updates the day of the week dropdown list */

  var selector = "#ByDOW";
  updateCombo(selector, DOWVals[0], DOWVals);
}


function updateDataAndPlot() {
  /* Updates the plotted data */

  routeId = $("#ByRoute").val();
  stopName = $("#ByStop").val();
  dayOfWeek = $("#ByDOW").val();

  if (routeId in SPEC_ROUTES) {
    routeId = SPEC_ROUTES[routeId]
  }

  $.ajax({
    url: "/mbta/api/delays-hourly",
    data: {
      routeId: routeId,
      stopName: stopName
    },
    success: function(result){
      if (plotHandler === undefined) {
        plotHandler = new FCPlot("#plot-div");
        plotHandler.initXLimsFromData(result);
        plotHandler.createPlot(dayOfWeek);
      }
      plotHandler.updatePlot(result, dayOfWeek);
    }
  });
}

function download_data() {
  /* Handles saving of data as a CSV file from the server */

  $.ajax({
    url: "/mbta/api/delays-hourly",
    headers: {
      "Accept": "text/csv",         
      "Content-Type": "text/csv"
    },
    data: {
      routeId: routeId,
      stopName: stopName
    },
    success: function(result){
      var anchor = $('#download-pseudo-link');
      anchor.attr({
        href: 'data:text/csv;base64,'+btoa(result),
        download: "delays-hourly.csv",
      });
      anchor.get(0).click();
    }
  });
}


$(function() {
  /* Document-Ready Event Handler */

  fillRoutes();
  $("#ByStop").val('')
  updateStopsList();
  fillDOW();

  $("#ByRoute").change(updateStopsList);

  $("#ByRoute").change(updateDataAndPlot);
  $("#ByStop")[0].addEventListener('autocomplete', updateDataAndPlot);
  $("#ByDOW").change(updateDataAndPlot);

  $("#BtnDownload").click(download_data);

  updateDataAndPlot();
});