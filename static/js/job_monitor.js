// job_monitor.js
import { initUI } from './ui_utils.js';
import {
  loadWatchers,
  filterAndDisplayWatchers,
  createWatcher,
  initWatchersPagination,
  forceWatcherRescan
} from './watchers.js';

import {
  loadJobs,
  sortAndDisplayJobs,
  prevJobsPage,
  nextJobsPage,
  setJobsPageSize,
  initJobsIconStyles,
  highlightSelectedJob
} from './jobs.js';

document.addEventListener('DOMContentLoaded', () => {
  // Initialize UI components
  initUI();

  // ========== Watchers ==========
   const watchersTable = document.getElementById('watchers-table');
  if (watchersTable) {
    console.log("Found watchers table, initializing watchers functionality");

    // Initialize pagination (this will now only happen once due to our flag)
    if (typeof initWatchersPagination === 'function') {
      initWatchersPagination();
    } else {
      console.warn("initWatchersPagination function not found - pagination may not work correctly");
    }

    // Load watchers initially (if not already loaded)
    if (Array.isArray(window.watchersData) && window.watchersData.length > 0) {
      console.log("Watchers data already loaded, skipping initial load");
    } else {
      loadWatchers();
    }

    // Set up refresh watchers button
    const refreshWatchersBtn = document.getElementById('refresh-watchers');
    if (refreshWatchersBtn) {
      // Clean up old listeners
      const newRefreshBtn = refreshWatchersBtn.cloneNode(true);
      refreshWatchersBtn.parentNode.replaceChild(newRefreshBtn, refreshWatchersBtn);

      // Add new listener
      newRefreshBtn.addEventListener('click', () => {
        console.log("Refreshing watchers");
        loadWatchers();
      });
    }

    // Set up status filter
    const statusFilter = document.getElementById('status-filter');
    if (statusFilter) {
      // Clean up old listeners
      const newStatusFilter = statusFilter.cloneNode(true);
      statusFilter.parentNode.replaceChild(newStatusFilter, statusFilter);

      // Add new listener
      newStatusFilter.addEventListener('change', () => {
        console.log(`Status filter changed to: ${newStatusFilter.value}`);
        loadWatchers();
      });
    }

    // Set up search
    const searchInput = document.getElementById('watcher-search');
    if (searchInput) {
      // Clean up old listeners
      const newSearchInput = searchInput.cloneNode(true);
      searchInput.parentNode.replaceChild(newSearchInput, searchInput);

      // Add new listener
      newSearchInput.addEventListener('input', () => {
        console.log("Watcher search input changed");
        filterAndDisplayWatchers();
      });
    }
  }

  // ========== Jobs ==========
  const jobsTable = document.getElementById('jobs-table');
  if (jobsTable) {
    loadJobs(); // initial load
    initJobsIconStyles(); // apply styles

    // Make sure to run highlightSelectedJob after jobs are loaded
    setTimeout(() => {
      highlightSelectedJob();
    }, 500); // Give a short delay to ensure jobs are rendered

    document.getElementById('jobs-prev-page')?.addEventListener('click', prevJobsPage);
    document.getElementById('jobs-next-page')?.addEventListener('click', nextJobsPage);

    document.getElementById('jobs-page-size')?.addEventListener('change', (e) => {
      setJobsPageSize(e.target.value);
    });

    // Sort job table on header click
    document.querySelectorAll('#jobs-table th[data-sort]').forEach(th => {
      th.addEventListener('click', () => {
        const sortBy = th.dataset.sort;
        sortAndDisplayJobs(sortBy);
        // Toggle sorting UI
        document.querySelectorAll('#jobs-table th').forEach(h => {
          h.classList.remove('sort-asc','sort-desc');
        });
        th.classList.toggle('sort-asc');
      });
    });
  }

  // Hook up the watcher form submission
  const watcherForm = document.getElementById('watcher-form');
  watcherForm?.addEventListener('submit', createWatcher);

  // Folder selection
  const selectFolderBtn = document.getElementById('select-folder-btn');
  const folderPathInput = document.getElementById('folder_path');

  if (selectFolderBtn && folderPathInput) {
    selectFolderBtn.addEventListener('click', async () => {
      try {
        const selectedDir = await window.pywebview.api.select_directory();
        if (selectedDir) {
          folderPathInput.value = selectedDir;
          console.log(`Selected folder: ${selectedDir}`);
        }
      } catch (error) {
        console.error('Error selecting directory:', error);
        window.showToast?.('Failed to open directory selector', 'error');
      }
    });
  }

  document.getElementById('refresh-jobs')?.addEventListener('click', () => loadJobs());

  // Auto-refresh for active pages
  setInterval(() => {
    if (document.visibilityState === 'visible') {
      if (watchersTable) loadWatchers(false);
      if (jobsTable) loadJobs(false);

      // Re-apply highlighting when auto-refreshing (for jobs page)
      if (jobsTable && window.location.search.includes('job_id')) {
        setTimeout(highlightSelectedJob, 100);
      }
    }
  }, 10000);
});

// In job_monitor.js, make functions globally accessible
import * as jobsFunctions from './jobs.js';
import * as watchersFunctions from './watchers.js';

// Make them available globally
window.loadJobs = jobsFunctions.loadJobs;
window.loadWatchers = watchersFunctions.loadWatchers;
window.highlightSelectedJob = jobsFunctions.highlightSelectedJob;
window.forceWatcherRescan = watchersFunctions.forceWatcherRescan;

// Listen for URL changes (for when navigating from dashboard to jobs page)
let lastUrl = location.href;
new MutationObserver(() => {
  const url = location.href;
  if (url !== lastUrl) {
    lastUrl = url;
    console.log('URL changed to', url);

    // If we're on the jobs page and there's a job_id in the URL
    if (document.querySelector('#jobs-table') && url.includes('job_id')) {
      // Give the jobs table time to render before highlighting
      setTimeout(highlightSelectedJob, 500);
    }
  }
}).observe(document, {subtree: true, childList: true});

console.log('Job Monitor JS loaded');