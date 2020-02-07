/*the autocomplete function takes two arguments,
the text field element and an array of possible autocompleted values:*/
function Autocomplete(inp, arr) {
  const MaxSuggest = 100;

  var _this = this;
  _this.values = arr;
  _this.elem = inp;
  _this.currentFocus = -1;

  _this.showSuggestions = function(e) {
    var a, b, i, val = _this.elem.value;
    /*close any already open lists of autocompleted values*/
    closeAllLists();
    _this.currentFocus = -1;
    /*create a DIV element that will contain the items (values):*/
    a = document.createElement("DIV");
    a.setAttribute("id", _this.elem.id + "autocomplete-list");
    a.setAttribute("class", "autocomplete-items");
    /*append the DIV element as a child of the autocomplete container:*/
    _this.elem.parentNode.appendChild(a);
    /*for each item in the array...*/
    for (i = 0; i < _this.values.length; i++) {
      item = _this.values[i];
      /*check if the item starts with the same letters as the text field value:*/
      if (item.substr(0, val.length).toUpperCase() == val.toUpperCase()) {
        /*create a DIV element for each matching element:*/
        b = document.createElement("DIV");
        /*make the matching letters bold:*/
        b.innerHTML = "<strong>" + item.substr(0, val.length) + "</strong>";
        b.innerHTML += item.substr(val.length);
        /*insert a input field that will hold the current array item's value:*/
        b.innerHTML += "<input type='hidden' value='" + item + "'>";
        /*execute a function when someone clicks on the item value (DIV element):*/
        b.addEventListener("click", function(e) {
            /*insert the value for the autocomplete text field:*/
            _this.elem.value = this.getElementsByTagName("input")[0].value;
            /*close the list of autocompleted values,
            (or any other open lists of autocompleted values:*/
            closeAllLists();
        });
        a.appendChild(b);
        if (a.childElementCount >= MaxSuggest)
          break;
      }
    }
  }
  _this.elem.addEventListener("input", function(e) {
    if (_this.elem.value)
      _this.showSuggestions(e);
  });
  _this.elem.addEventListener("dblclick", function(e) {
    if (!_this.elem.value)
      _this.showSuggestions(e)
  });

  _this.keydownHandler = function(e) {
    var x = document.getElementById(_this.elem.id + "autocomplete-list");
    if (x) x = x.getElementsByTagName("div");
    if (e.keyCode == 40) {
      /*If the arrow DOWN key is pressed,
      increase the currentFocus variable:*/
      _this.currentFocus++;
      /*and and make the current item more visible:*/
      _this.addActive(x);
    } else if (e.keyCode == 38) { //up
      /*If the arrow UP key is pressed,
      decrease the currentFocus variable:*/
      _this.currentFocus--;
      /*and and make the current item more visible:*/
      _this.addActive(x);
    } else if (e.keyCode == 13) {
      /*If the ENTER key is pressed, prevent the form from being submitted,*/
      e.preventDefault();
      if (_this.currentFocus > -1) {
        /*and simulate a click on the "active" item:*/
        if (x) x[_this.currentFocus].click();
      }
    }
  }
  _this.elem.addEventListener("keydown", _this.keydownHandler);

  _this.addActive = function(x) {
    /*a function to classify an item as "active":*/
    if (!x) return false;
    /*start by removing the "active" class on all items:*/
    _this.removeActive(x);
    if (_this.currentFocus >= x.length)
      _this.currentFocus = 0;
    if (_this.currentFocus < 0)
      _this.currentFocus = (x.length - 1);
    /*add class "autocomplete-active":*/
    x[_this.currentFocus].classList.add("autocomplete-active");
  }

  _this.removeActive = function(x) {
    /*a function to remove the "active" class from all autocomplete items:*/
    for (var i = 0; i < x.length; i++) {
      x[i].classList.remove("autocomplete-active");
    }
  }

  function closeAllLists(elmnt) {
    /*close all autocomplete lists in the document,
    except the one passed as an argument:*/
    var x = document.getElementsByClassName("autocomplete-items");
    for (var i = 0; i < x.length; i++) {
      if (elmnt != x[i] && elmnt != inp) {
        x[i].parentNode.removeChild(x[i]);
      }
    }
  }
  /*execute a function when someone clicks in the document:*/
  document.addEventListener("click", function (e) {
      closeAllLists(e.target);
  });
}
