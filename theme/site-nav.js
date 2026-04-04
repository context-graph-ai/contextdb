// Inject site-wide navigation bar at top of docs pages
// Matches the contextdb.tech landing page nav
(function() {
  var bar = document.createElement('div');
  bar.id = 'site-nav';
  bar.innerHTML = '<a href="/" class="sn-wordmark">' +
    '<svg viewBox="0 0 100 100" width="22" height="22" xmlns="http://www.w3.org/2000/svg"><defs><clipPath id="sna"><circle cx="50" cy="36.14" r="30"/></clipPath><clipPath id="snb"><circle cx="38" cy="56.93" r="30"/></clipPath><clipPath id="snc"><circle cx="62" cy="56.93" r="30"/></clipPath></defs><circle cx="50" cy="36.14" r="30" fill="#38bdf8" fill-opacity="0.1" stroke="#38bdf8" stroke-width="1.5" stroke-opacity="0.45"/><circle cx="38" cy="56.93" r="30" fill="#38bdf8" fill-opacity="0.1" stroke="#38bdf8" stroke-width="1.5" stroke-opacity="0.45"/><circle cx="62" cy="56.93" r="30" fill="#38bdf8" fill-opacity="0.1" stroke="#38bdf8" stroke-width="1.5" stroke-opacity="0.45"/><g clip-path="url(#snb)"><circle cx="50" cy="36.14" r="30" fill="#38bdf8" fill-opacity="0.18"/></g><g clip-path="url(#snc)"><circle cx="50" cy="36.14" r="30" fill="#38bdf8" fill-opacity="0.18"/></g><g clip-path="url(#snc)"><circle cx="38" cy="56.93" r="30" fill="#38bdf8" fill-opacity="0.18"/></g><g clip-path="url(#sna)"><g clip-path="url(#snb)"><circle cx="62" cy="56.93" r="30" fill="#38bdf8" fill-opacity="0.85"/></g></g></svg>' +
    ' contextdb</a>' +
    '<div class="sn-links">' +
    '<a href="/">Home</a>' +
    '<a href="https://github.com/context-graph-ai/contextdb">GitHub</a>' +
    '<a href="/#early-access" class="sn-cta">Get Early Access</a>' +
    '</div>';
  document.body.insertBefore(bar, document.body.firstChild);
})();
