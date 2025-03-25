const toggleButton = document.getElementById('toggle-btn')
const sidebar = document.getElementById('sidebar')

function toggleSidebar(){
  sidebar.classList.toggle('close')
  toggleButton.classList.toggle('rotate')

  closeAllSubMenus()
}

function toggleSubMenu(button){

  if(!button.nextElementSibling.classList.contains('show')){
    closeAllSubMenus()
  }

  button.nextElementSibling.classList.toggle('show')
  button.classList.toggle('rotate')

  if(sidebar.classList.contains('close')){
    sidebar.classList.toggle('close')
    toggleButton.classList.toggle('rotate')
  }
}

function closeAllSubMenus(){
  Array.from(sidebar.getElementsByClassName('show')).forEach(ul => {
    ul.classList.remove('show')
    ul.previousElementSibling.classList.remove('rotate')
  })
}

function applyTheme(theme) {
  if (theme === 'light') {
    document.documentElement.classList.add('light-mode');
    document.getElementById('theme-toggle').textContent = '🌙';
  } else {
    document.documentElement.classList.remove('light-mode');
    document.getElementById('theme-toggle').textContent = '☀️';
  }
}

document.addEventListener("DOMContentLoaded", function () {
  const savedTheme = localStorage.getItem('theme') || 'dark';
  applyTheme(savedTheme);

  document.getElementById('theme-toggle').addEventListener('click', () => {
    const current = document.documentElement.classList.contains('light-mode') ? 'light' : 'dark';
    const newTheme = current === 'light' ? 'dark' : 'light';
    localStorage.setItem('theme', newTheme);
    applyTheme(newTheme);
  });
});
