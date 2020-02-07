const NoFilterRoutes = "";
const DOWVals = [
  "", "Weekdays", "Weekends", "Mondays", "Tuesdays",
  "Wednesdays", "Fridays", "Saturdays", "Sundays"
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
  $.ajax({url: "/api/routeids", success: function(data){
    routeIds = [ NoFilterRoutes ].concat(data.items);
    updateCombo(selector, NoFilterRoutes, routeIds);
  }});
}

function updateStopsList() {
  routeId = $("#ByRoute").val();
  $.ajax({
    url: "/api/stopnames",
    data: {
      routeId: routeId
    },
    success: function(result){
      stopsAutocomplete.values = result.items;
    }
  });
}

function fillDOW() {
  var selector = "#ByDOW";
  updateCombo(selector, DOWVals[0], DOWVals);
}


$("#ByRoute").change(updateStopsList);


$(function() {
  fillRoutes();
  $("#ByStop").val('')
  updateStopsList();
  fillDOW();
});