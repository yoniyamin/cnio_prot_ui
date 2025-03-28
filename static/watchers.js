/************************************
 * watchers.js
 * Contains all code to load watchers,
 * filter/search watchers, paginate,
 * sort watchers, expand watchers, etc.
 ************************************/

// ===== Global variables for watchers =====
let watchersData = [];
let filteredWatchers = [];
let expandedWatchers = new Set();

let currentWatcherSort = { column: 'id', ascending: false };
let currentPage = 1;
const itemsPerPage = 10;

// ====== Fetch watchers from server ======
export function loadWatchers(showLoading = true) {
  const tableBody = document.querySelector('#watchers-table tbody');
  if (!tableBody) return;

  if (showLoading) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="8" class="loading-message">
          <div class="loading-spinner"></div>
          <span>Loading watchers...</span>
        </td>
      </tr>
    `;
  }

  const statusFilter = document.getElementById('status-filter')?.value || 'all';
  let apiUrl = '/api/watchers';
  if (statusFilter !== 'all') {
    apiUrl += `?status=${statusFilter}`;
  }

  fetch(apiUrl)
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        tableBody.innerHTML = `
          <tr>
            <td colspan="8" class="loading-message">
              <span class="text-red-500">Error: ${data.error}</span>
            </td>
          </tr>`;
        return;
      }

      watchersData = data.watchers || [];
      filterAndDisplayWatchers();
      reExpandWatchers();
    })
    .catch((error) => {
      console.error('Error loading watchers:', error);
      tableBody.innerHTML = `
        <tr>
          <td colspan="8" class="loading-message">
            <span class="text-red-500">Failed to load watchers</span>
          </td>
        </tr>`;
    });
}

// ====== Filter watchers by search term ======
export function filterAndDisplayWatchers() {
  const searchTerm = document.getElementById('watcher-search')?.value.toLowerCase() || '';

  if (searchTerm) {
    filteredWatchers = watchersData.filter((w) => {
      const fields = [
        w.id.toString(),
        w.job_name_prefix,
        w.folder_path,
        w.file_pattern,
        w.status,
      ];
      return fields.some((field) => field.toLowerCase().includes(searchTerm));
    });
  } else {
    filteredWatchers = [...watchersData];
  }

  sortWatchers(); // will call displayWatchers() once sorting is done
  currentPage = 1; // reset to first page
}

// ====== Sort watchers table ======
function sortWatchers() {
  filteredWatchers.sort((a, b) => {
    const { column, ascending } = currentWatcherSort;
    let aValue, bValue;

    switch (column) {
      case 'id':
        aValue = a.id;
        bValue = b.id;
        break;
      case 'name':
        aValue = a.job_name_prefix;
        bValue = b.job_name_prefix;
        break;
      case 'status':
        aValue = a.status;
        bValue = b.status;
        break;
      case 'folder_path':
        aValue = a.folder_path;
        bValue = b.folder_path;
        break;
      case 'file_pattern':
        aValue = a.file_pattern;
        bValue = b.file_pattern;
        break;
      case 'progress':
        aValue = calcProgressValue(a);
        bValue = calcProgressValue(b);
        break;
      case 'creation_time':
        aValue = new Date(a.creation_time);
        bValue = new Date(b.creation_time);
        break;
      default:
        aValue = a.id;
        bValue = b.id;
    }

    if (aValue < bValue) return ascending ? -1 : 1;
    if (aValue > bValue) return ascending ? 1 : -1;
    return 0;
  });

  displayWatchers();
}

// ====== Display watchers with pagination ======
function displayWatchers() {
  const tableBody = document.querySelector('#watchers-table tbody');
  if (!tableBody) return;

  const totalPages = Math.ceil(filteredWatchers.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const pageItems = filteredWatchers.slice(startIndex, endIndex);

  // Update pagination UI
  document.getElementById('current-page').textContent = currentPage;
  document.getElementById('total-pages').textContent = totalPages;
  document.getElementById('prev-page').disabled = (currentPage <= 1);
  document.getElementById('next-page').disabled = (currentPage >= totalPages);

  // Clear table
  tableBody.innerHTML = '';

  // If empty
  if (pageItems.length === 0) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="8" class="loading-message">
          <span>No watchers found</span>
        </td>
      </tr>`;
    return;
  }

  // Populate rows
  pageItems.forEach((watcher) => {
    const row = createWatcherRow(watcher);
    tableBody.appendChild(row);
  });
}

// ====== Create a single watcher row ======
function createWatcherRow(watcher) {
  const row = document.createElement('tr');
  row.className = 'watcher-row hover:bg-hover';
  row.dataset.id = watcher.id;

  const progressValue = calcProgressValue(watcher);
  const progressText = calcProgressText(watcher, progressValue);
  const formattedDate = formatDate(watcher.creation_time);

  // We detect if "new UI" (badges) or "old UI" (colored dots) is in use
  const usingNewUI = document.querySelector('.status-badge') !== null;

  if (usingNewUI) {
    row.innerHTML = `
      <td>${watcher.id}</td>
      <td>${watcher.job_name_prefix}</td>
      <td>
        <span class="status-badge ${watcher.status}">${watcher.status}</span>
      </td>
      <td title="${watcher.folder_path}">${watcher.folder_path}</td>
      <td>${watcher.file_pattern}</td>
      <td>
        <div class="progress-container">
          <div class="progress-bar">
            <div class="progress-fill" style="width: ${progressValue}%"></div>
          </div>
          <div class="progress-text">${progressText}</div>
        </div>
      </td>
      <td>${formattedDate}</td>
      <td>
        <div class="action-icons">
          <button class="action-icon info toggle-details" title="Show Files">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-5 w-5">
              <path fill-rule="evenodd" 
                    d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293
                       a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4
                       a1 1 0 010-1.414z" 
                    clip-rule="evenodd" />
            </svg>
          </button>
          ${
            watcher.status === 'monitoring'
              ? `<button class="action-icon terminate stop-watcher" title="Terminate Watcher">
                  <svg xmlns="http://www.w3.org/2000/svg" 
                       viewBox="0 0 20 20" fill="currentColor" 
                       class="h-5 w-5">
                    <path fill-rule="evenodd" 
                          d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293
                             a1 1 0 111.414 1.414L11.414 10l4.293 4.293
                             a1 1 0 01-1.414 1.414L10 11.414l-4.293 
                             4.293a1 1 0 01-1.414-1.414L8.586 10
                             4.293 5.707a1 1 0 010-1.414z" 
                          clip-rule="evenodd" />
                  </svg>
                </button>`
              : ''
          }
        </div>
      </td>
    `;
  } else {
    // Old UI variant
    const statusColor = getStatusColor(watcher.status);
    row.innerHTML = `
      <td class="p-2 border">${watcher.id}</td>
      <td class="p-2 border">${watcher.job_name_prefix}</td>
      <td class="p-2 border">
        <div class="flex items-center gap-2">
          <div class="w-3 h-3 rounded-full ${statusColor}"></div>
          <span class="capitalize">${watcher.status}</span>
        </div>
      </td>
      <td class="p-2 border">${watcher.folder_path}</td>
      <td class="p-2 border">${watcher.file_pattern}</td>
      <td class="p-2 border">
        <div class="w-full bg-gray-200 rounded-full h-2.5">
          <div class="bg-accent h-2.5 rounded-full" style="width: ${progressValue}%"></div>
        </div>
        <div class="text-xs mt-1">${progressText}</div>
      </td>
      <td class="p-2 border">${formattedDate}</td>
      <td class="p-2 border text-center">
        <div class="flex justify-center gap-1">
          <button class="expand-watcher" title="Show Files">
            <svg xmlns="http://www.w3.org/2000/svg"
                 class="h-5 w-5" viewBox="0 0 24 24" 
                 stroke="currentColor" fill="none">
              <path stroke-linecap="round" stroke-linejoin="round" 
                    stroke-width="2" 
                    d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          ${
            watcher.status === 'monitoring'
              ? `<button class="stop-watcher" title="Stop Watcher">
                   <svg xmlns="http://www.w3.org/2000/svg"
                        class="h-5 w-5" fill="none" 
                        viewBox="0 0 24 24" stroke="currentColor">
                     <path stroke-linecap="round" stroke-linejoin="round"
                           stroke-width="2" 
                           d="M6 18L18 6M6 6l12 12" />
                   </svg>
                 </button>`
              : ''
          }
        </div>
      </td>
    `;
  }

  // Expand / collapse details
  const expandBtn = row.querySelector('.toggle-details, .expand-watcher');
  expandBtn?.addEventListener('click', () => toggleWatcherDetails(watcher.id));

  // Stop watcher
  const stopBtn = row.querySelector('.stop-watcher');
  stopBtn?.addEventListener('click', () => confirmStopWatcher(watcher.id));

  return row;
}

// ====== Expand/collapse details row ======
function toggleWatcherDetails(watcherId) {
  const detailsRow = document.querySelector(`.watcher-details-row[data-watcher-id="${watcherId}"]`);

  if (detailsRow) {
    // Currently open; remove it
    detailsRow.remove();
    expandedWatchers.delete(watcherId);
    revertExpandIcon(watcherId);
    return;
  }

  expandedWatchers.add(watcherId);
  updateExpandIcon(watcherId);

  // Insert details row
  const watcherRow = document.querySelector(`.watcher-row[data-id="${watcherId}"]`);
  if (!watcherRow) return;

  const template = document.getElementById('watcher-details-template');
  const rowClone = document.importNode(template.content, true).querySelector('.watcher-details-row');
  rowClone.dataset.watcherId = watcherId;

  // Refresh button inside the details
  const refreshBtn = rowClone.querySelector('.refresh-files');
  refreshBtn?.addEventListener('click', () => loadWatcherFiles(watcherId, rowClone));

  // Insert after the main row
  watcherRow.parentNode.insertBefore(rowClone, watcherRow.nextSibling);

  // Load the files for this watcher
  loadWatcherFiles(watcherId, rowClone);
}

function reExpandWatchers() {
  // If some watchers were expanded, expand them again
  expandedWatchers.forEach((id) => {
    const row = document.querySelector(`.watcher-row[data-id="${id}"]`);
    if (row) toggleWatcherDetails(id);
  });
}

// ====== Load watcher files ======
function loadWatcherFiles(watcherId, detailsRow) {
  const tableBody = detailsRow.querySelector('.files-table tbody');
  if (!tableBody) return;

  // Show a loading message
  tableBody.innerHTML = `
    <tr>
      <td colspan="5" class="loading-message">
        <div class="loading-spinner"></div>
        <span>Loading files...</span>
      </td>
    </tr>`;

  fetch(`/api/watchers/${watcherId}/files`)
    .then((res) => res.json())
    .then((data) => {
      if (data.error) {
        tableBody.innerHTML = `
          <tr><td colspan="5" class="loading-message">
            <span class="text-red-500">Error: ${data.error}</span>
          </td></tr>`;
        return;
      }

      const files = data.files || [];
      if (!files.length) {
        tableBody.innerHTML = `
          <tr><td colspan="5" class="loading-message">
            <span>No files captured yet</span>
          </td></tr>`;
        return;
      }

      // Clear and populate
      tableBody.innerHTML = '';
      files.forEach((file) => {
        const row = createFileRow(file);
        tableBody.appendChild(row);
      });

      // Hook up sorting on file table, etc.
      initFileTableSort(detailsRow, files);
    })
    .catch((err) => {
      console.error('Error loading files:', err);
      tableBody.innerHTML = `
        <tr><td colspan="5" class="loading-message">
          <span class="text-red-500">Failed to load files</span>
        </td></tr>`;
    });
}

// ====== Create file row ======
function createFileRow(file) {
  const row = document.createElement('tr');
  row.className = 'hover:bg-hover';

  const captureDate = file.capture_time ? formatDate(file.capture_time) : 'Pending';
  const fileStatus = file.status || (file.capture_time ? 'captured' : 'pending');

  // Detect new or old UI
  const usingNewUI = document.querySelector('.status-badge') !== null;
  if (usingNewUI) {
    row.innerHTML = `
      <td title="${file.file_name}">${file.file_name}</td>
      <td title="${file.file_path || 'N/A'}">${file.file_path || 'N/A'}</td>
      <td>${captureDate}</td>
      <td>${file.job_id || 'N/A'}</td>
      <td class="text-center">
        <span class="status-badge ${fileStatus}">${fileStatus}</span>
      </td>
    `;
  } else {
    const statusHtml =
      fileStatus === 'pending'
        ? `<span class="inline-block bg-yellow-100 text-yellow-800 text-xs rounded-full px-2 py-1">Pending</span>`
        : `<span class="inline-block bg-green-100 text-green-800 text-xs rounded-full px-2 py-1">Captured</span>`;
    row.innerHTML = `
      <td class="p-2 border">${file.file_name}</td>
      <td class="p-2 border">${file.file_path || 'N/A'}</td>
      <td class="p-2 border">${captureDate}</td>
      <td class="p-2 border">${file.job_id || 'N/A'}</td>
      <td class="p-2 border text-center">${statusHtml}</td>
    `;
  }

  return row;
}

// ====== Sorting within the file table ======
function initFileTableSort(detailsRow, filesArray) {
  const headers = detailsRow.querySelectorAll('.files-table th[data-sort]');
  headers.forEach((th) => {
    th.addEventListener('click', () => {
      const sortBy = th.dataset.sort;
      const isAsc = !th.classList.contains('sort-asc');

      // Clear old sort classes
      headers.forEach((h) => h.classList.remove('sort-asc', 'sort-desc'));
      th.classList.add(isAsc ? 'sort-asc' : 'sort-desc');

      // Sort
      filesArray.sort((a, b) => compareFiles(a, b, sortBy, isAsc));
      // Re-render
      const tbody = detailsRow.querySelector('.files-table tbody');
      tbody.innerHTML = '';
      filesArray.forEach((file) => tbody.appendChild(createFileRow(file)));
    });
  });
}

function compareFiles(a, b, column, ascending) {
  let aValue, bValue;
  switch (column) {
    case 'file_name':
      aValue = a.file_name;
      bValue = b.file_name;
      break;
    case 'file_path':
      aValue = a.file_path || '';
      bValue = b.file_path || '';
      break;
    case 'capture_time':
      aValue = a.capture_time ? new Date(a.capture_time) : new Date(0);
      bValue = b.capture_time ? new Date(b.capture_time) : new Date(0);
      break;
    case 'job_id':
      aValue = a.job_id || '';
      bValue = b.job_id || '';
      break;
    default:
      aValue = a.file_name;
      bValue = b.file_name;
  }
  if (aValue < bValue) return ascending ? -1 : 1;
  if (aValue > bValue) return ascending ? 1 : -1;
  return 0;
}

// ====== Stop watcher confirmation ======
function confirmStopWatcher(watcherId) {
  // If there's a custom modal:
  const confirmModal = document.getElementById('confirm-modal');
  if (confirmModal) {
    showConfirmationModal(
      `Are you sure you want to terminate watcher #${watcherId}?`,
      () => stopWatcher(watcherId) // callback
    );
  } else {
    if (confirm('Are you sure you want to stop this watcher?')) {
      stopWatcher(watcherId);
    }
  }
}

// ====== Actually stop the watcher ======
function stopWatcher(watcherId) {
  fetch(`/api/watchers/${watcherId}/update-status`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'cancelled' }),
  })
    .then((r) => r.json())
    .then((result) => {
      if (result.success) {
        showToast?.(`Watcher #${watcherId} terminated successfully`, 'success');
        loadWatchers();
      } else {
        const msg = `Error terminating watcher: ${result.error || 'Unknown error'}`;
        showToast?.(msg, 'error') || alert(msg);
      }
    })
    .catch((err) => {
      console.error('Error stopping watcher:', err);
      showToast?.('Error terminating watcher. Please try again.', 'error') ||
        alert('Error terminating watcher. Please try again.');
    });
}

// ====== Create a new watcher ======
export function createWatcher(event) {
  event.preventDefault();
  const form = event.target;
  const folderPath = form.folder_path.value;
  const filePattern = form.file_pattern.value;
  const jobType = form.job_type.value;
  const jobDemands = form.job_demands.value;
  const jobNamePrefix = form.job_name_prefix.value || 'Auto_Job';

  if (!folderPath || !filePattern) {
    const msg = 'Please fill in both folder path and file pattern';
    showToast?.(msg, 'error') || alert(msg);
    return;
  }

  const submitBtn = form.querySelector('button[type="submit"]');
  const originalText = submitBtn.textContent;
  submitBtn.disabled = true;
  submitBtn.textContent = 'Creating...';

  fetch('/api/watchers', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      folder_path: folderPath,
      file_pattern: filePattern,
      job_type: jobType,
      job_demands: jobDemands,
      job_name_prefix: jobNamePrefix
    }),
  })
    .then((r) => r.json())
    .then((result) => {
      submitBtn.disabled = false;
      submitBtn.textContent = originalText;

      if (result.success) {
        form.reset();
        // Close modal (if new UI or old UI)
        const modal = document.getElementById('create-watcher-modal');
        closeModal?.(modal) || (modal.classList.add('hidden'));

        loadWatchers();
        showToast?.('Watcher created successfully', 'success');
      } else {
        const msg = `Error creating watcher: ${result.error || 'Unknown error'}`;
        showToast?.(msg, 'error') || alert(msg);
      }
    })
    .catch((err) => {
      console.error('Error creating watcher:', err);
      submitBtn.disabled = false;
      submitBtn.textContent = originalText;
      showToast?.('Error creating watcher. Please try again.', 'error') ||
        alert('Error creating watcher. Please try again.');
    });
}

// ====== Utility functions ======
function calcProgressValue(watcher) {
  if (!watcher) return 0;
  if (watcher.expected_count > 0) {
    return (watcher.captured_count / watcher.expected_count) * 100;
  }
  return watcher.captured_count > 0 ? 100 : 0;
}

function calcProgressText(watcher, progressValue) {
  if (watcher.expected_count > 0) {
    return `${watcher.captured_count}/${watcher.expected_count} (${Math.round(progressValue)}%)`;
  }
  return watcher.captured_count > 0
    ? `${watcher.captured_count} files`
    : 'No files yet';
}

function formatDate(dateString) {
  if (!dateString) return 'N/A';
  const d = new Date(dateString);
  return d.toLocaleString();
}

function getStatusColor(status) {
  switch (status) {
    case 'monitoring': return 'bg-green-500';
    case 'completed':  return 'bg-blue-500';
    case 'cancelled':  return 'bg-red-500';
    case 'paused':     return 'bg-yellow-500';
    default:           return 'bg-gray-500';
  }
}

function revertExpandIcon(watcherId) {
  const row = document.querySelector(`.watcher-row[data-id="${watcherId}"]`);
  if (!row) return;
  const icon = row.querySelector('.toggle-details svg, .expand-watcher svg');
  if (!icon) return;
  // Choose whichever default arrow you had
  if (icon.closest('.toggle-details')) {
    // Possibly new UI
    icon.innerHTML = '<path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />';
  } else {
    icon.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />';
  }
}

function updateExpandIcon(watcherId) {
  const row = document.querySelector(`.watcher-row[data-id="${watcherId}"]`);
  if (!row) return;
  const icon = row.querySelector('.toggle-details svg, .expand-watcher svg');
  if (!icon) return;
  // Choose whichever "expanded" arrow you had
  if (icon.closest('.toggle-details')) {
    // Possibly new UI
    icon.innerHTML = '<path fill-rule="evenodd" d="M14.707 12.707a1 1 0 01-1.414 0L10 9.414l-3.293 3.293a1 1 0 01-1.414-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 010 1.414z" clip-rule="evenodd" />';
  } else {
    icon.innerHTML = '<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />';
  }
}

// ====== Pagination button helpers ======
export function prevWatchersPage() {
  if (currentPage > 1) {
    currentPage--;
    displayWatchers();
  }
}

export function nextWatchersPage() {
  const totalPages = Math.ceil(filteredWatchers.length / itemsPerPage);
  if (currentPage < totalPages) {
    currentPage++;
    displayWatchers();
  }
}
