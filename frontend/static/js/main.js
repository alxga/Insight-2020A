SPEC_ROUTES = {
  "": "",
  "Buses Only": "ALLBUSES",
  "Trains Only": "ALLTRAINS"
}
const DOWVals = [
  "", "Weekdays", "Weekends"
]

var stopsAutocomplete = new Autocomplete($("#ByStop")[0], []);


function updateCombo(selector, choice, arr) {
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
  var selector = "#ByRoute";
  $.ajax({url: "/mbta/api/routeids", success: function(data){
    routeIds = Object.keys(SPEC_ROUTES).concat(data.items);
    updateCombo(selector, "", routeIds);
  }});
}

function updateStopsList() {
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
  var selector = "#ByDOW";
  updateCombo(selector, DOWVals[0], DOWVals);
}


function updateDataAndPlot() {
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
      stopName: stopName,
      dayOfWeek: dayOfWeek
    },
    success: function(result){
      updatePlot(result);
    }
  });
}


$(function() {
  fillRoutes();
  $("#ByStop").val('')
  updateStopsList();
  fillDOW();

  $("#ByRoute").change(updateStopsList);

  $("#ByRoute").change(updateDataAndPlot);
  $("#ByStop")[0].addEventListener('autocomplete', updateDataAndPlot);
  $("#ByDOW").change(updateDataAndPlot);
  updateDataAndPlot();
});