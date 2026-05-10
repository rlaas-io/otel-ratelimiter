(function () {
  "use strict";

  var toggle = document.querySelector(".nav-toggle");
  var links = document.querySelector(".nav-links");

  if (!toggle || !links) {
    return;
  }

  toggle.addEventListener("click", function () {
    links.classList.toggle("open");
  });

  document.addEventListener("click", function (e) {
    if (!e.target.closest(".nav-inner")) {
      links.classList.remove("open");
    }
  });
})();
