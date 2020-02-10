function Autocomplete(inp, arr) {
  /* Text autocompletion helper

    The object constructor function takes as input a text field element and
    an array of possible autocompleted values
    Upon a selection being made or cancelled the object updates the element
    and triggers an 'autocomplete' custom event
  */
  var _this = this;

  // Maximum number of suggested values
  const MaxSuggest = 100;

  // Autocompletion values and the text field element
  _this.values = arr;
  _this.elem = inp;
  // Index of the currently highlighted autocompletion suggestion if any
  _this.currentFocus = -1;

  _this.showSuggestions = function() {
    /* Presents a list of suggested autocompletion values */

    var a, b, i, val = _this.elem.value;
    closeAllLists();
    _this.currentFocus = -1;

    // Create a DIV element that will contain the items (values)
    a = document.createElement('DIV');
    a.setAttribute('id', _this.elem.id + 'autocomplete-list');
    a.setAttribute('class', 'autocomplete-items');
    _this.elem.parentNode.appendChild(a);

    // Fill the DIV with the suggested values
    for (i = 0; i < _this.values.length; i++) {
      item = _this.values[i];
      if (item.substr(0, val.length).toUpperCase() == val.toUpperCase()) {

        // Create a DIV element for each matching element
        b = document.createElement('DIV');
        b.innerHTML = "<strong>" + item.substr(0, val.length) + "</strong>";
        b.innerHTML += item.substr(val.length);
        b.innerHTML += "<input type='hidden' value='" + item + "'>";
        
        // Execute a function when someone clicks on a suggested value
        b.addEventListener('click', function() {
            _this.elem.value = this.getElementsByTagName('input')[0].value;
            closeAllLists();
            _this.elem.dispatchEvent(new CustomEvent('autocomplete'));
        });
        a.appendChild(b);

        if (a.childElementCount >= MaxSuggest)
          break;
      }
    }
  }
  _this.elem.addEventListener('input', function() {
      _this.showSuggestions();
  });
  _this.elem.addEventListener('dblclick', function() {
    if (!_this.elem.value)
      _this.showSuggestions()
  });

  _this.keydownHandler = function(e) {
    /* Handles arrow-up and arrow-down for scrolling,
      enter to autocomplete a highlighted value, and tab to cancel the input
    */

    var x = document.getElementById(_this.elem.id + 'autocomplete-list');
    if (x) x = x.getElementsByTagName('div');
    if (e.keyCode == 40) {
      // If the arrow DOWN key is pressed, increase the currentFocus variable
      // and and make the current item more visible:
      _this.currentFocus++;
      _this.addActive(x, false);
    } else if (e.keyCode == 38) { 
      // If the arrow DOWN key is pressed, decrease the currentFocus variable
      // and and make the current item more visible:
      _this.currentFocus--;
      _this.addActive(x, true);
    } else if (e.keyCode == 13) {
      // If the ENTER key is pressed, prevent the form from being submitted
      // and simulate a click on the 'active' item
      e.preventDefault();
      if (_this.currentFocus > -1) {
        if (x) x[_this.currentFocus].click();
      }
    } else if (e.keyCode == 9) {
      // If the TAB key is pressed, close all suggestions and clear the input
      // if there were any items, don't clear the input if the list wasn't shown
      // in the first place
      if (!closeAllLists()) {
        _this.elem.value = '';
        _this.elem.dispatchEvent(new CustomEvent('autocomplete'));
      }
    }
  }
  _this.elem.addEventListener('keydown', _this.keydownHandler);

  _this.isScrolledIntoView = function(elem)
  {
    /* Checks if the element is in the viewport of its parent

      Returns:
        Boolean indicating whether the element in in the viewport
    */

    var docViewTop = 0;
    var docViewBottom = docViewTop + elem.parentNode.clientHeight;

    var elemTop = $(elem).position().top;
    var elemBottom = elemTop + $(elem).outerHeight();

    return ((elemBottom <= docViewBottom) && (elemTop >= docViewTop));
  }

  _this.addActive = function(x, alignScrollTo) {
    /* Highlights the currently selected suggestion */

    if (!x) return false;
    _this.removeActive(x);
    if (_this.currentFocus >= x.length)
      _this.currentFocus = 0;
    if (_this.currentFocus < 0)
      _this.currentFocus = (x.length - 1);
    x[_this.currentFocus].classList.add("autocomplete-active");
    if (!_this.isScrolledIntoView(x[_this.currentFocus]))
      x[_this.currentFocus].scrollIntoView(alignScrollTo);
  }

  _this.removeActive = function(x) {
    /* Removes the highlighting from all suggestions */

    for (var i = 0; i < x.length; i++) {
      x[i].classList.remove('autocomplete-active');
    }
  }

  function closeAllLists(elmnt) {
    /* Clears the list of suggestions optionally keeping one element

      Args:
        elmnt: element to keep

      Returns:
        True if an element was kept or if the list was empty
        False otherwise
    */
    var x = document.getElementsByClassName('autocomplete-items');
    var haveSelection = false;
    var removedCount = 0;
    for (var i = 0; i < x.length; i++) {
      if (elmnt == x[i]) {
        haveSelection = true;
      }
      else if (elmnt != _this.elem) {
        x[i].parentNode.removeChild(x[i]);
        removedCount ++;
      }
    }
    return haveSelection || removedCount <= 0;
  }

  document.addEventListener('click', function (e) {
    /* Handles mouse click events, clears the element if the user clicks
      outside of the suggestions list
    */
    if (!closeAllLists(e.target)) {
      _this.elem.value = '';
      _this.elem.dispatchEvent(new CustomEvent('autocomplete'));
    }
  });
}
