/************************************
 * job_monitor.js
 * Small "main" file that just wires
 * up watchers.js and jobs.js together.
 ************************************/

import {
  loadWatchers,
  filterAndDisplayWatchers,
  createWatcher,
  prevWatchersPage,
  nextWatchersPage,
  setWatchersPageSize
} from './watchers.js';

import {
  loadJobs,
  sortAndDisplayJobs,
  prevJobsPage,
  nextJobsPage,
  setJobsPageSize
} from './jobs.js';

function openModal(modal) {
  if (!modal) return;
  modal.classList.add('active');
  document.body.style.overflow = 'hidden'; // Prevent background scrolling
}

function closeModal(modal) {
  if (!modal) return;
  modal.classList.remove('active');
  document.body.style.overflow = ''; // Restore scrolling
}

// Make sure openModal and closeModal are available globally
window.openModal = function(modal) {
  if (!modal) return;
  modal.classList.add('active');
  document.body.style.overflow = 'hidden'; // Prevent background scrolling
};

window.closeModal = function(modal) {
  if (!modal) return;
  modal.classList.remove('active');
  document.body.style.overflow = ''; // Restore scrolling
};

document.addEventListener('DOMContentLoaded', () => {
  // ========== Watchers ==========
  loadWatchers(); // initial load
  // Set up refresh watchers
  const refreshWatchersBtn = document.getElementById('refresh-watchers');
  refreshWatchersBtn?.addEventListener('click', () => loadWatchers());

  // Set up status filter
  const statusFilter = document.getElementById('status-filter');
  statusFilter?.addEventListener('change', () => loadWatchers());
  // Pagination watchers
  document.getElementById('prev-page')?.addEventListener('click', () => prevWatchersPage());
  document.getElementById('next-page')?.addEventListener('click', () => nextWatchersPage());

  document.getElementById('watcher-page-size')?.addEventListener('change', (e) => {
    setWatchersPageSize(e.target.value);
  });
  // Set up search
  const searchInput = document.getElementById('watcher-search');
  if (searchInput) {
    // You can call a debounce if you want, or directly
    searchInput.addEventListener('input', () => filterAndDisplayWatchers());
  }

  const selectFolderBtn = document.getElementById('select-folder-btn');
  const folderPathInput = document.getElementById('folder_path');

  if (selectFolderBtn && folderPathInput) {
    selectFolderBtn.addEventListener('click', async () => {
      try {
        // Call the webview API to open directory selection
        const selectedDir = await window.pywebview.api.select_directory();
        if (selectedDir) {
          folderPathInput.value = selectedDir;
          console.log(`Selected folder: ${selectedDir}`);
        } else {
          console.log('Directory selection canceled');
        }
      } catch (error) {
        console.error('Error selecting directory:', error);
        showToast?.('Failed to open directory selector', 'error');
      }
    });
  } else {
    console.error('Browse button or folder input not found');
  }

  // Set up sorting on watchers table header
  document.querySelectorAll('#watchers-table th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      // We rely on watchers.js internal logic,
      // but we can just call the same function if you want:
      const sortBy = th.dataset.sort;
      // watchers.js handles toggling ascending/descending internally.
      // In the refactor above, you might have to create a function
      // that sets currentWatcherSort externally. Or just unify it there.
      // For simplicity, let's just call filterAndDisplayWatchers()
      // if watchers.js does that sorting automatically inside.

      // (If you need direct control, define a watchers.js function
      //  like setWatcherSort(sortBy).)
    });
  });

  // Pagination watchers
  document.getElementById('prev-page')?.addEventListener('click', () => prevWatchersPage());
  document.getElementById('next-page')?.addEventListener('click', () => nextWatchersPage());

  // Create watcher modal
  const createWatcherBtn = document.getElementById('create-watcher-btn');
  const createWatcherModal = document.getElementById('create-watcher-modal');

   if (createWatcherBtn && createWatcherModal) {
    createWatcherBtn.addEventListener('click', () => {
      console.log('Opening modal'); // Debug
      openModal(createWatcherModal);
    });
  } else {
    console.error('Create watcher button or modal not found:',
                 { btn: !!createWatcherBtn, modal: !!createWatcherModal });
  }

  const closeModalBtn = document.getElementById('close-modal');
  if (closeModalBtn && createWatcherModal) {
    closeModalBtn.addEventListener('click', () => {
      closeModal(createWatcherModal);
    });
  }

  const cancelWatcherBtn = document.getElementById('cancel-watcher');
  if (cancelWatcherBtn && createWatcherModal) {
    cancelWatcherBtn.addEventListener('click', () => {
      closeModal(createWatcherModal);
    });
  }

  window.showToast = function(message, type = 'success', duration = 3000) {
    const toast = document.getElementById('toast-notification');
    if (!toast) return;

    const toastContent = toast.querySelector('.toast-content');
    const toastMessage = toast.querySelector('.toast-message');

    // Reset classes
    toastContent.className = 'toast-content';
    if (type) {
      toastContent.classList.add(type);
    }

    toastMessage.textContent = message;

    // Show the toast
    toast.classList.add('active');

    // Set up auto-dismiss
    const timeout = setTimeout(() => {
      toast.classList.remove('active');
    }, duration);

    // Handle manual close
    const closeBtn = toast.querySelector('.toast-close');
    if (closeBtn) {
      closeBtn.onclick = () => {
        clearTimeout(timeout);
        toast.classList.remove('active');
      };
    }
  };

  // Confirmation modal function
  window.showConfirmationModal = function(message, confirmCallback) {
    const modal = document.getElementById('confirm-modal');
    if (!modal) return;

    const messageEl = document.getElementById('confirm-message');
    if (messageEl) messageEl.textContent = message;

    const confirmBtn = document.getElementById('confirm-action');
    if (confirmBtn) {
      // Store original handler
      const originalHandler = confirmBtn.onclick;

      // Set new handler
      confirmBtn.onclick = () => {
        closeModal(modal);
        confirmCallback();

        // Reset to original handler
        confirmBtn.onclick = originalHandler;
      };
    }

    // Set up cancel buttons
    document.querySelectorAll('.confirm-cancel').forEach(btn => {
      btn.onclick = () => {
        closeModal(modal);
      };
    });

    openModal(modal);
  };





  // Hook up the watcher form submission
  const watcherForm = document.getElementById('watcher-form');
  watcherForm?.addEventListener('submit', createWatcher);

  // ========== Jobs ==========
  loadJobs(); // initial load
  document.getElementById('jobs-prev-page')?.addEventListener('click', () => prevJobsPage());
  document.getElementById('jobs-next-page')?.addEventListener('click', () => nextJobsPage());
  document.getElementById('jobs-page-size')?.addEventListener('change', (e) => {
    setJobsPageSize(e.target.value);
  });
  // Sort job table on header click
  document.querySelectorAll('#jobs-table th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const sortBy = th.dataset.sort;
      sortAndDisplayJobs(sortBy);
      // Add some UI toggling of sort classes if you want
      document.querySelectorAll('#jobs-table th').forEach(h => {
        h.classList.remove('sort-asc','sort-desc');
      });
      // Toggle logic or something:
      th.classList.toggle('sort-asc');
    });
  });

  // (You can also set up an interval to auto-refresh watchers/jobs):
  setInterval(() => {
    if (document.visibilityState === 'visible') {
      loadWatchers(false);
      loadJobs(false);
    }
  }, 10000);
});

console.log('Job Monitor JS loaded');

// Log API request errors
window.fetchWithLogging = function(url, options = {}) {
  console.log(`Fetching: ${url}`, options);

  return fetch(url, options)
    .then(response => {
      console.log(`Response from ${url}:`, response.status);
      return response.json();
    })
    .then(data => {
      console.log(`Data from ${url}:`, data);
      return data;
    })
    .catch(error => {
      console.error(`Error fetching ${url}:`, error);
      throw error;
    });
};
