/* QRA Watch — Theme toggle (banknim pattern) */
function getSystemTheme() {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}
function getSavedTheme() {
  try { return localStorage.getItem('qra-theme'); } catch (e) { return null; }
}
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  try { localStorage.setItem('qra-theme', theme); } catch (e) {}
  var btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = theme === 'dark' ? '\u2600' : '\u263E';
}
var _qraTheme = getSavedTheme() || 'light';
applyTheme(_qraTheme);
document.addEventListener('DOMContentLoaded', function () { applyTheme(_qraTheme); });
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function (e) {
  if (!getSavedTheme()) applyTheme(e.matches ? 'dark' : 'light');
});
window.qraToggleTheme = function () {
  var cur = document.documentElement.getAttribute('data-theme') || getSystemTheme();
  applyTheme(cur === 'dark' ? 'light' : 'dark');
};
