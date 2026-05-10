/* OTEL docs global nav + dropdown controller */
(function () {
  'use strict';

  function currentPage() {
    var path = window.location.pathname.split('/').pop();
    return path || 'index.html';
  }

  function isActive(page, targets) {
    return targets.indexOf(page) !== -1 ? 'active' : '';
  }

  function renderNav() {
    var navInner = document.querySelector('.nav-inner');
    if (!navInner) return;

    var page = currentPage();
    navInner.innerHTML = [
      '<a href="index.html" class="nav-logo">OTEL Rate Limiter</a>',
      '<button class="nav-toggle" aria-label="Menu">',
      '  <svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>',
      '</button>',
      '<ul class="nav-links">',
      '  <li><a href="index.html" class="' + isActive(page, ['index.html']) + '">Overview</a></li>',
      '  <li>',
      '    <button class="nav-dropdown-toggle">Product <svg class="chevron" viewBox="0 0 10 10" fill="none"><path d="M2 4l3 3 3-3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></button>',
      '    <div class="nav-dropdown-menu nav-mega nav-mega-cols">',
      '      <div class="nav-mega-group">',
      '        <h5>Platform</h5>',
      '        <ul>',
      '          <li><a href="architecture.html" class="' + isActive(page, ['architecture.html']) + '">Architecture<small>Data + control plane design</small></a></li>',
      '          <li><a href="behavior-diagrams.html" class="' + isActive(page, ['behavior-diagrams.html']) + '">Behavior Diagrams<small>Before vs after runtime flow</small></a></li>',
      '          <li><a href="change-impact.html" class="' + isActive(page, ['change-impact.html']) + '">Change Impact<small>Business and ops outcomes</small></a></li>',
      '          <li><a href="policies.html" class="' + isActive(page, ['policies.html']) + '">Policies<small>Policy design and rollout safety</small></a></li>',
      '        </ul>',
      '      </div>',
      '      <div class="nav-mega-group">',
      '        <h5>Operations</h5>',
      '        <ul>',
      '          <li><a href="admin-api.html" class="' + isActive(page, ['admin-api.html']) + '">Admin API<small>Health, stats, reload, metrics</small></a></li>',
      '          <li><a href="monitoring.html" class="' + isActive(page, ['monitoring.html']) + '">Monitoring<small>Prometheus and alerting</small></a></li>',
      '          <li><a href="configuration.html" class="' + isActive(page, ['configuration.html']) + '">Configuration<small>Full production config reference</small></a></li>',
      '        </ul>',
      '      </div>',
      '    </div>',
      '  </li>',
      '  <li>',
      '    <button class="nav-dropdown-toggle">Developers <svg class="chevron" viewBox="0 0 10 10" fill="none"><path d="M2 4l3 3 3-3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></button>',
      '    <div class="nav-dropdown-menu nav-mega">',
      '      <div class="nav-mega-group">',
      '        <h5>Build and Use</h5>',
      '        <ul>',
      '          <li><a href="getting-started.html" class="' + isActive(page, ['getting-started.html']) + '">Getting Started<small>Install and first deployment</small></a></li>',
      '          <li><a href="examples.html" class="' + isActive(page, ['examples.html']) + '">Examples<small>Production-ready config snippets</small></a></li>',
      '        </ul>',
      '      </div>',
      '    </div>',
      '  </li>',
      '  <li><a href="getting-started.html" class="nav-cta">Get Started</a></li>',
      '</ul>'
    ].join('');
  }

  function setupDropdowns() {
    var CLOSE_DELAY = 120;
    var items = document.querySelectorAll('.nav-links > li');

    function closeAll() {
      items.forEach(function (li) { li.classList.remove('dd-open'); });
    }

    function isMobile() {
      return window.innerWidth <= 768;
    }

    items.forEach(function (li) {
      var toggle = li.querySelector('.nav-dropdown-toggle');
      if (!toggle) return;

      var timer = null;

      function open() {
        clearTimeout(timer);
        items.forEach(function (other) {
          if (other !== li) other.classList.remove('dd-open');
        });
        li.classList.add('dd-open');
      }

      function scheduleClose() {
        timer = setTimeout(function () { li.classList.remove('dd-open'); }, CLOSE_DELAY);
      }

      li.addEventListener('mouseenter', function () {
        if (!isMobile()) open();
      });
      li.addEventListener('mouseleave', function () {
        if (!isMobile()) scheduleClose();
      });

      toggle.addEventListener('click', function (e) {
        e.preventDefault();
        e.stopPropagation();
        if (li.classList.contains('dd-open')) {
          li.classList.remove('dd-open');
        } else {
          open();
        }
      });
    });

    document.addEventListener('click', function (e) {
      if (!e.target.closest('.nav-links')) closeAll();
    });

    var hamburger = document.querySelector('.nav-toggle');
    if (hamburger) {
      hamburger.addEventListener('click', function () {
        var links = document.querySelector('.nav-links');
        if (links) links.classList.toggle('open');
      });
    }

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') closeAll();
    });
  }

  renderNav();
  setupDropdowns();
})();
