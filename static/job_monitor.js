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
  nextWatchersPage
} from './watchers.js';

import {
  loadJobs,
  sortAndDisplayJobs
} from './jobs.js';

document.addEventListener('DOMContentLoaded', () => {
  // ========== Watchers ==========
  loadWatchers(); // initial load
  // Set up refresh watchers
  const refreshWatchersBtn = document.getElementById('refresh-watchers');
  refreshWatchersBtn?.addEventListener('click', () => loadWatchers());

  // Set up status filter
  const statusFilter = document.getElementById('status-filter');
  statusFilter?.addEventListener('change', () => loadWatchers());

  // Set up search
  const searchInput = document.getElementById('watcher-search');
  if (searchInput) {
    // You can call a debounce if you want, or directly
    searchInput.addEventListener('input', () => filterAndDisplayWatchers());
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
  createWatcherBtn?.addEventListener('click', () => openModal?.(createWatcherModal));

  const closeModalBtn = document.getElementById('close-modal');
  closeModalBtn?.addEventListener('click', () => closeModal?.(createWatcherModal));

  const cancelWatcherBtn = document.getElementById('cancel-watcher');
  cancelWatcherBtn?.addEventListener('click', () => closeModal?.(createWatcherModal));

  // Hook up the watcher form submission
  const watcherForm = document.getElementById('watcher-form');
  watcherForm?.addEventListener('submit', createWatcher);

  // ========== Jobs ==========
  loadJobs(); // initial load
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
