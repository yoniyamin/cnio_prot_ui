/************************************
 * watchers.js
 * Contains all code to load watchers,
 * filter/search watchers, paginate,
 * sort watchers, expand watchers, etc.
 ************************************/

import { createProgressBar, formatDate } from './utils.js';
let paginationInitialized = false;
// ===== Global variables for watchers =====
let watchersData = [];
let filteredWatchers = [];
let expandedWatchers = new Set();

let currentWatcherSort = { column: 'id', ascending: false };
let currentPage = 1;
let itemsPerPage = 8;

export function setWatchersPageSize(size) {
  itemsPerPage = parseInt(size, 10) || 8;
  currentPage = 1; // Reset to first page when changing page size
  updatePagination();
  displayWatchers(getPaginatedWatchers());

  // Re-expand any expanded watchers
  setTimeout(reExpandWatchers, 100);
}

// ====== Fetch watchers from server ======
export function loadWatchers(showLoading = true) {
  const tableBody = document.querySelector('#watchers-table tbody');
  if (!tableBody) return;

  if (showLoading) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="7" class="loading-message">
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

  console.log("Fetching watchers from:", apiUrl);

  fetch(apiUrl)
    .then((response) => {
      console.log(`Watchers API response status: ${response.status}`);
      return response.json();
    })
    .then((data) => {
      console.log("Watchers API full response:", data);

      // Log structure analysis to help debug property names
      if (data.watchers && data.watchers.length > 0) {
        const watcher = data.watchers[0];
        console.log("First watcher data:", watcher);

        // Extract and log all property names available in the watcher object
        console.log("All watcher properties:", Object.keys(watcher));

        // Check for common progress-related properties
        console.log("Progress-related properties found:", {
          processed_files: watcher.processed_files,
          total_files: watcher.total_files,
          captured_count: watcher.captured_count,
          expected_count: watcher.expected_count,
          progress_percent: watcher.progress_percent,
          // Look deeper for nested properties
          files_array: Array.isArray(watcher.files) ? watcher.files.length : null,
          expected_files: Array.isArray(watcher.expected_files) ? watcher.expected_files.length : null
        });

        // Check for file tracking in the watcher object
        if (watcher.files) {
          console.log("Watcher has 'files' property:", watcher.files);
        }
      }

      if (data.error) {
        tableBody.innerHTML = `
          <tr>
            <td colspan="7" class="loading-message">
              <span class="text-red-500">Error: ${data.error}</span>
            </td>
          </tr>`;
        return;
      }

      // Check for alternate data structures
      let watchersArray = [];
      if (Array.isArray(data)) {
        // Direct array response
        watchersArray = data;
      } else if (data.watchers && Array.isArray(data.watchers)) {
        // Object with watchers array
        watchersArray = data.watchers;
      } else {
        // Try to find any array property
        const arrayProps = Object.keys(data).filter(key => Array.isArray(data[key]));
        if (arrayProps.length > 0) {
          watchersArray = data[arrayProps[0]];
        } else {
          console.error("Could not find watchers array in response", data);
          watchersArray = [];
        }
      }

      watchersData = watchersArray;
      filterAndDisplayWatchers();
      reExpandWatchers();
    })
    .catch((error) => {
      console.error('Error loading watchers:', error);
      tableBody.innerHTML = `
        <tr>
          <td colspan="7" class="loading-message">
            <span class="text-red-500">Failed to load watchers: ${error.message}</span>
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
  // Use the global variables rather than function parameters
  const sortBy = currentWatcherSort.column;
  const ascending = currentWatcherSort.ascending;

  // Sort the filtered watchers array based on current sort settings
  if (Array.isArray(filteredWatchers)) {
    filteredWatchers.sort((a, b) => {
      // Add null checks to handle missing properties
      if (!a || !b) return 0;

      let valA = a[sortBy];
      let valB = b[sortBy];

      // Handle missing values
      if (valA === undefined) valA = '';
      if (valB === undefined) valB = '';

      // Compare based on data types
      if (typeof valA === 'string' && typeof valB === 'string') {
        return ascending ? valA.localeCompare(valB) : valB.localeCompare(valA);
      } else {
        return ascending ? valA - valB : valB - valA;
      }
    });
  }

  // Update pagination and display the sorted results
  updatePagination();
  displayWatchers(getPaginatedWatchers());
}



function getPaginatedWatchers() {
  if (!Array.isArray(filteredWatchers)) {
    return [];
  }

  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  return filteredWatchers.slice(startIndex, endIndex);
}

// Add a function to update pagination UI
function updatePagination() {
  const totalPages = Math.ceil((filteredWatchers?.length || 0) / itemsPerPage);

  // Update page info
  const currentPageEl = document.getElementById('current-page');
  const totalPagesEl = document.getElementById('total-pages');
  const pageSizeEl = document.getElementById('watcher-page-size');

  if (currentPageEl) currentPageEl.textContent = currentPage;
  if (totalPagesEl) totalPagesEl.textContent = totalPages;
  if (pageSizeEl) pageSizeEl.value = itemsPerPage.toString();

  // Enable/disable pagination buttons
  const prevBtn = document.getElementById('prev-page');
  const nextBtn = document.getElementById('next-page');

  if (prevBtn) prevBtn.disabled = currentPage <= 1;
  if (nextBtn) nextBtn.disabled = currentPage >= totalPages;

  console.log(`Pagination updated: Page ${currentPage}/${totalPages}, showing ${itemsPerPage} items per page`);
}


// ====== Display watchers with pagination ======
function displayWatchers(watchers) {
  const tbody = document.querySelector('#watchers-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  // Add null check to prevent error
  if (!watchers || watchers.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7; // Update to match your actual column count
    cell.textContent = 'No watchers found.';
    cell.className = 'empty-table-message';
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  watchers.forEach(watcher => {
    // Skip if watcher is null or undefined
    if (!watcher) return;

    const row = document.createElement('tr');
    row.dataset.watcherId = watcher.id;
    row.className = 'watcher-row'; // Add this class for identifying rows

    // ID column
    const idCell = document.createElement('td');
    idCell.className = 'col-id';
    idCell.textContent = watcher.id;
    row.appendChild(idCell);

    // Name column
    const nameCell = document.createElement('td');
    nameCell.className = 'col-name';
    nameCell.textContent = watcher.job_name_prefix || `Watcher ${watcher.id}`;
    row.appendChild(nameCell);

    // Status column
    const statusCell = document.createElement('td');
    statusCell.className = 'col-status';
    const statusBadge = document.createElement('span');
    const status = (watcher.status || 'unknown').toLowerCase();
    statusBadge.className = `status-badge ${status}`;
    statusBadge.textContent = watcher.status || 'Unknown';
    statusCell.appendChild(statusBadge);
    row.appendChild(statusCell);

    // File Pattern column
    const patternCell = document.createElement('td');
    patternCell.className = 'col-pattern';
    patternCell.textContent = watcher.file_pattern || '';
    row.appendChild(patternCell);

    // Progress column with new progress bar
    const progressCell = document.createElement('td');
    progressCell.className = 'col-progress';

    // Calculate progress based on available data
    // Try multiple property name combinations that might exist in the API response
    let processedFiles = 0;
    let totalFiles = 0;

    // Option 1: captured_count / expected_count
    if (watcher.captured_count !== undefined && watcher.expected_count !== undefined) {
      processedFiles = watcher.captured_count;
      totalFiles = watcher.expected_count;
    }
    // Option 2: processed_files / total_files
    else if (watcher.processed_files !== undefined && watcher.total_files !== undefined) {
      processedFiles = watcher.processed_files;
      totalFiles = watcher.total_files;
    }
    // Option 3: captured_files (array length) / expected_files (array length)
    else if (Array.isArray(watcher.captured_files) && Array.isArray(watcher.expected_files)) {
      processedFiles = watcher.captured_files.length;
      totalFiles = watcher.expected_files.length;
    }
    // Option 4: Use the explicit progress_percent if available
    else if (watcher.progress_percent !== undefined) {
      processedFiles = watcher.progress_percent;
      totalFiles = 100;
    }

    // Fallback: Check if the watcher has a files property with captured_count
    else if (watcher.files && watcher.files.captured_count !== undefined) {
      processedFiles = watcher.files.captured_count;
      totalFiles = watcher.files.expected_count || 0;
    }

    // Calculate percentage
    const percentComplete = totalFiles > 0 ? Math.round((processedFiles / totalFiles) * 100) : 0;

    // Create label with file counts
    const progressLabel = `${processedFiles}/${totalFiles} files`;

    // Add the progress bar to the cell
    progressCell.appendChild(createProgressBar(percentComplete, progressLabel));
    row.appendChild(progressCell);

    // Created column
    const createdCell = document.createElement('td');
    createdCell.className = 'col-created';
    createdCell.textContent = watcher.creation_time ? formatDate(watcher.creation_time) : 'N/A';
    row.appendChild(createdCell);

    // Actions column
    const actionsCell = document.createElement('td');
    actionsCell.className = 'col-actions';
    const actionsDiv = document.createElement('div');
    actionsDiv.className = 'action-icons';

    // Toggle details button
    const toggleButton = document.createElement('button');
    toggleButton.className = 'action-icon info toggle-details';
    toggleButton.title = 'Show details';
    toggleButton.innerHTML = '<i class="fas fa-info-circle"></i>';
    toggleButton.dataset.id = watcher.id; // Add data-id attribute for event handling

    // Stop watcher button (if monitoring)
    const stopButton = document.createElement('button');
    stopButton.className = 'action-icon terminate stop-watcher';
    stopButton.title = 'Stop watcher';
    stopButton.innerHTML = '<i class="fas fa-stop-circle"></i>';
    stopButton.dataset.id = watcher.id; // Add data-id attribute for event handling

    // Only show stop button if the watcher is active
    if (status === 'monitoring') {
      actionsDiv.appendChild(stopButton);
    }

    actionsDiv.appendChild(toggleButton);
    actionsCell.appendChild(actionsDiv);
    row.appendChild(actionsCell);

    tbody.appendChild(row);
  });

  // Add event listeners after creating the rows
  attachRowEventListeners();

  // Update pagination UI
  updatePagination();

  console.log(`Displayed ${watchers.length} watchers on page ${currentPage}`);
}

function attachRowEventListeners() {
  // Attach click handlers for toggle details buttons
  document.querySelectorAll('.toggle-details').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation(); // Prevent event bubbling
      const watcherId = btn.dataset.id;
      if (watcherId) {
        toggleWatcherDetails(watcherId);
      }
    });
  });

  // Attach click handlers for stop buttons
  document.querySelectorAll('.stop-watcher').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation(); // Prevent event bubbling
      const watcherId = btn.dataset.id;
      if (watcherId) {
        confirmStopWatcher(watcherId);
      }
    });
  });

  // Optional: Make entire rows clickable for expansion
  document.querySelectorAll('.watcher-row').forEach(row => {
    row.addEventListener('click', (e) => {
      // Only toggle if not clicking on action buttons
      if (!e.target.closest('.action-icon, .stop-watcher, .toggle-details')) {
        const watcherId = row.dataset.watcherId;
        if (watcherId) {
          toggleWatcherDetails(watcherId);
        }
      }
    });
  });
}


// ====== Create a single watcher row ======
function createWatcherRow(watcher) {
  const row = document.createElement('tr');
  row.className = 'watcher-row hover:bg-hover';
  row.dataset.id = watcher.id;
  row.style.cursor = 'pointer'; // Indicate clickable

  // Make entire row clickable for expansion
  row.addEventListener('click', (e) => {
    // Only toggle if not clicking on action buttons
    if (!e.target.closest('.action-icon, .stop-watcher, .toggle-details, .expand-watcher')) {
      toggleWatcherDetails(watcher.id);
    }
  });

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
              <img src="/images/listfiles.png" alt="Show Files" class="toggle-details" title="Show Files">
              ${
        watcher.status === 'monitoring'
            ? `<img src="/static/images/stop.png" alt="Terminate" class="stop-watcher" title="Terminate Watcher">`
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
          <button class="action-icon info toggle-details" title="Show Files">
          <img src="/static/images/listfiles.png" alt="Show Files" class="h-5 w-5">
        </button>
          ${
          watcher.status === 'monitoring'
            ? `<button class="action-icon terminate stop-watcher" title="Terminate Watcher">
                <img src="/static/images/stop.png" alt="Terminate" class="h-5 w-5">
              </button>`
            : ''
        }
        </div>
      </td>
    `;
  }

  // Expand / collapse details
  const expandBtn = row.querySelector('.toggle-details, .expand-watcher');
  expandBtn?.addEventListener('click', (e) => {
    e.stopPropagation(); // Prevent row click from bubbling
    toggleWatcherDetails(watcher.id);
  });

  // Stop watcher
  const stopBtn = row.querySelector('.stop-watcher');
  stopBtn?.addEventListener('click', (e) => {
    e.stopPropagation(); // Prevent row click from bubbling
    confirmStopWatcher(watcher.id);
  });

  return row;
}

// ====== Expand/collapse details row ======
function toggleWatcherDetails(watcherId) {
  console.log(`Toggling details for watcher ID: ${watcherId}`);
  const detailsRow = document.querySelector(`.watcher-details-row[data-watcher-id="${watcherId}"]`);

  if (detailsRow) {
    // Currently open; remove it
    detailsRow.remove();
    expandedWatchers.delete(watcherId);
    // Optional: revert icon
    return;
  }

  expandedWatchers.add(watcherId);
  // Optional: update icon

  // Insert details row
  const watcherRow = document.querySelector(`tr[data-watcher-id="${watcherId}"]`);
  if (!watcherRow) {
    console.error(`Could not find watcher row with ID: ${watcherId}`);
    return;
  }

  // Use the template to create the details row
  const template = document.getElementById('watcher-details-template');
  if (!template) {
    console.error("Details template not found");
    return;
  }

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
  console.log("Re-expanding watcher details for:", Array.from(expandedWatchers));

  // For each expanded watcher ID
  expandedWatchers.forEach((watcherId) => {
    // Check if this watcher is currently visible on this page
    const watcherRow = document.querySelector(`tr[data-watcher-id="${watcherId}"]`);

    if (watcherRow) {
      console.log(`Re-expanding watcher ${watcherId} details`);
      // If the details row doesn't exist yet, create it
      if (!document.querySelector(`.watcher-details-row[data-watcher-id="${watcherId}"]`)) {
        toggleWatcherDetails(watcherId);
      }
    } else {
      console.log(`Watcher ${watcherId} not on current page, can't expand`);
    }
  });
}

// ====== Load watcher files ======
// Enhanced loadWatcherFiles function with improved debugging and display logic
function loadWatcherFiles(watcherId, detailsRow) {
  const tableBody = detailsRow.querySelector('.files-table tbody');
  if (!tableBody) {
    console.error("Files table body not found in details row");
    return;
  }

  // Show a loading message
  tableBody.innerHTML = `
    <tr>
      <td colspan="4" class="loading-message">
        <div class="loading-spinner"></div>
        <span>Loading files...</span>
      </td>
    </tr>`;

  console.log(`Fetching files for watcher ID: ${watcherId}`);

  fetch(`/api/watchers/${watcherId}/files`)
    .then((response) => {
      console.log(`Files API response status: ${response.status}`);
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      console.log("Files API full response:", data);

      if (data.error) {
        tableBody.innerHTML = `
          <tr><td colspan="4" class="loading-message">
            <span class="text-red-500">Error: ${data.error}</span>
          </td></tr>`;
        return;
      }

      // Handle different possible response structures with better logging
      let files = [];
      if (Array.isArray(data)) {
        console.log("Response is a direct array of files");
        files = data;
      } else if (data.files && Array.isArray(data.files)) {
        console.log("Response has 'files' property with array of files");
        files = data.files;
      } else if (data.captured_files && Array.isArray(data.captured_files)) {
        console.log("Response has 'captured_files' property with array of files");
        files = data.captured_files;
      } else {
        // Try to extract files from any property that is an array
        console.log("Searching for any array property in response");
        const arrayProps = Object.keys(data).filter(key => Array.isArray(data[key]));
        if (arrayProps.length > 0) {
          console.log(`Found array property: ${arrayProps[0]}`);
          files = data[arrayProps[0]];
        } else {
          console.error("Could not find files array in response", data);
        }
      }

      console.log(`Found ${files.length} files`, files);

      // Debug empty files array
      if (!files || files.length === 0) {
        console.log("No files found in response", data);
        tableBody.innerHTML = `
          <tr><td colspan="4" class="loading-message">
            <span>No files captured yet</span>
          </td></tr>`;
        return;
      }

      // Clear and populate with better error handling
      try {
        tableBody.innerHTML = '';

        files.forEach((file) => {
          if (!file) {
            console.warn("Skipping undefined or null file entry");
            return;
          }

          console.log("Creating row for file:", file);
          const row = createFileRow(file);
          tableBody.appendChild(row);
        });

        // Hook up sorting on file table
        initFileTableSort(detailsRow, files);

        console.log("Successfully populated files table");
      } catch (err) {
        console.error("Error creating file rows:", err);
        tableBody.innerHTML = `
          <tr><td colspan="4" class="loading-message">
            <span class="text-red-500">Error displaying files: ${err.message}</span>
          </td></tr>`;
      }
    })
    .catch((err) => {
      console.error('Error loading files:', err);
      tableBody.innerHTML = `
        <tr><td colspan="4" class="loading-message">
          <span class="text-red-500">Failed to load files: ${err.message}</span>
          <button class="action-button secondary retry-button">Retry</button>
        </td></tr>`;

      // Add retry functionality
      const retryButton = tableBody.querySelector('.retry-button');
      if (retryButton) {
        retryButton.addEventListener('click', () => loadWatcherFiles(watcherId, detailsRow));
      }
    });
}

export function initWatchersPagination() {
  if (paginationInitialized) {
    console.log("Watchers pagination already initialized, skipping");
    return;
  }

  console.log("Initializing watchers pagination event listeners");

  // Remove any existing listeners first (to be safe)
  const prevBtn = document.getElementById('prev-page');
  const nextBtn = document.getElementById('next-page');
  const pageSizeSelect = document.getElementById('watcher-page-size');

  if (prevBtn) {
    // Remove old listeners by cloning and replacing the button
    const newPrevBtn = prevBtn.cloneNode(true);
    prevBtn.parentNode.replaceChild(newPrevBtn, prevBtn);

    // Add new listener
    newPrevBtn.addEventListener('click', function(e) {
      e.preventDefault();
      e.stopPropagation();
      console.log("Previous button clicked");
      prevWatchersPage();
    });
  }

  if (nextBtn) {
    // Remove old listeners by cloning and replacing the button
    const newNextBtn = nextBtn.cloneNode(true);
    nextBtn.parentNode.replaceChild(newNextBtn, nextBtn);

    // Add new listener
    newNextBtn.addEventListener('click', function(e) {
      e.preventDefault();
      e.stopPropagation();
      console.log("Next button clicked");
      nextWatchersPage();
    });
  }

  if (pageSizeSelect) {
    // Remove old listeners by cloning and replacing the select
    const newPageSizeSelect = pageSizeSelect.cloneNode(true);
    pageSizeSelect.parentNode.replaceChild(newPageSizeSelect, pageSizeSelect);

    // Add new listener
    newPageSizeSelect.addEventListener('change', function(e) {
      console.log(`Page size changed to ${this.value}`);
      setWatchersPageSize(parseInt(this.value, 10));
    });
  }

  paginationInitialized = true;
}
// ====== Create file row ======
function createFileRow(file) {
  console.log("Creating row for file:", file);

  const row = document.createElement('tr');
  row.className = 'hover:bg-hover';

  // Handle different possible file object structures
  const fileName = file.file_name || file.name || file.filename ||
                  (typeof file === 'string' ? file : 'Unknown');

  const filePath = file.file_path || file.path || file.fullPath || 'N/A';
  const jobId = file.job_id || file.jobId || 'N/A';

  // MODIFIED: Default to "captured" status instead of "pending" if no status is found
  // This assumes that any file shown in the list has been captured
  const fileStatus = file.status || file.state || 'captured';

  // Create cells for the column structure
  const nameCell = document.createElement('td');
  nameCell.title = fileName;
  nameCell.textContent = fileName;
  row.appendChild(nameCell);

  const pathCell = document.createElement('td');
  pathCell.title = filePath;
  pathCell.textContent = filePath;
  row.appendChild(pathCell);

  const jobCell = document.createElement('td');
  jobCell.textContent = jobId;
  row.appendChild(jobCell);

  const statusCell = document.createElement('td');
  const statusBadge = document.createElement('span');
  statusBadge.className = `status-badge ${fileStatus}`;
  statusBadge.textContent = fileStatus;
  statusCell.appendChild(statusBadge);
  row.appendChild(statusCell);

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
        loadWatchers(); // Refresh UI
      } else {
        showToast?.(`Error terminating watcher: ${result.error || 'Unknown error'}`, 'error');
      }
    })
    .catch((err) => {
      console.error('Error stopping watcher:', err);
      showToast?.('Error terminating watcher. Please try again.', 'error');
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


// Add this CSS to your document when it loads
document.addEventListener('DOMContentLoaded', () => {
  // Add styles to make sure SVGs inside action buttons don't interfere with click events
  const style = document.createElement('style');
  style.textContent = `
    .toggle-details,
    .stop-watcher {
      width: 1.25rem;
      height: 1.25rem;
      background: none;
      border: none;
      padding: 0;
      margin: 0;
      cursor: pointer;
    }
    .data-table tbody tr:hover .action-icons img {
      background: transparent;
    }
  `;
  document.head.appendChild(style);

  console.log('Action icons:', document.querySelectorAll('.action-icon').length);
  console.log('Toggle details:', document.querySelectorAll('.toggle-details').length);
  console.log('Watchers table:', !!document.querySelector('#watchers-table'));

});

document.addEventListener('DOMContentLoaded', () => {
  // If watchers-table is found on this page, let's auto-load watchers
  if (document.querySelector('#watchers-table')) {
    console.log("Found watchers-table, auto-calling loadWatchers()");
    loadWatchers();
  }
});

document.addEventListener('DOMContentLoaded', () => {
  const createWatcherBtn = document.getElementById('create-watcher-btn');
  const modal = document.getElementById('create-watcher-modal');
  if (createWatcherBtn && modal) {
    createWatcherBtn.addEventListener('click', () => {
      modal.classList.add('active');
    });
  }
});

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
    console.log(`Moving from page ${currentPage} to ${currentPage - 1}`);
    currentPage--;
    updatePagination();
    displayWatchers(getPaginatedWatchers());

    // Re-expand any expanded watchers after page change
    setTimeout(reExpandWatchers, 100);
  }
}

export function nextWatchersPage() {
  const totalPages = Math.ceil((filteredWatchers?.length || 0) / itemsPerPage);
  if (currentPage < totalPages) {
    console.log(`Moving from page ${currentPage} to ${currentPage + 1}`);
    currentPage++;
    updatePagination();
    displayWatchers(getPaginatedWatchers());

    // Re-expand any expanded watchers after page change
    setTimeout(reExpandWatchers, 100);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  if (document.querySelector('#watchers-table')) {
    console.log("Setting up watchers functionality");

    // Initialize pagination once
    initWatchersPagination();

    // Initial load of watchers
    loadWatchers();
  }
});

// Force a watcher to rescan its folder
window.forceWatcherRescan = function(watcherId) {
  console.log(`Forcing rescan for watcher ${watcherId}`);
  
  // First show a loading indicator
  const detailsRow = document.querySelector(`.watcher-details-row[data-watcher-id="${watcherId}"]`);
  if (detailsRow) {
    const tableBody = detailsRow.querySelector('.files-table tbody');
    if (tableBody) {
      tableBody.innerHTML = `
        <tr>
          <td colspan="4" class="loading-message">
            <div class="loading-spinner"></div>
            <span>Rescanning folder...</span>
          </td>
        </tr>`;
    }
  }

  // Call the API endpoint
  fetch(`/api/watchers/${watcherId}/rescan`, {
    method: 'POST',
  })
    .then((response) => response.json())
    .then((data) => {
      console.log("Rescan response:", data);
      if (data.success) {
        showToast?.('Folder rescan triggered successfully', 'success');
        // Wait a moment for files to be processed, then refresh the list
        setTimeout(() => {
          // Refresh the files list after scan
          if (detailsRow) {
            loadWatcherFiles(watcherId, detailsRow);
          }
        }, 1000);
      } else {
        showToast?.(`Error: ${data.error || 'Failed to rescan folder'}`, 'error');
        // Refresh the file list anyway after an error
        if (detailsRow) {
          loadWatcherFiles(watcherId, detailsRow);
        }
      }
    })
    .catch((error) => {
      console.error('Error triggering rescan:', error);
      showToast?.('Error communicating with server', 'error');
      // Refresh the file list anyway after an error
      if (detailsRow) {
        loadWatcherFiles(watcherId, detailsRow);
      }
    });
};

document.addEventListener('DOMContentLoaded', () => {
  // Set up event delegation for rescan button
  document.body.addEventListener('click', (e) => {
    if (e.target.closest('.rescan-folder')) {
      const detailsRow = e.target.closest('.watcher-details-row');
      if (detailsRow) {
        const watcherId = detailsRow.dataset.watcherId;
        if (watcherId) {
          // Call the function directly since we're in the same module
          window.forceWatcherRescan(watcherId);
        }
      }
    }
  });
});

// Force a watcher to rescan its folder
window.forceWatcherRescan = function(watcherId) {
  console.log(`Forcing rescan for watcher ${watcherId}`);
  
  // First show a loading indicator
  const detailsRow = document.querySelector(`.watcher-details-row[data-watcher-id="${watcherId}"]`);
  if (detailsRow) {
    const tableBody = detailsRow.querySelector('.files-table tbody');
    if (tableBody) {
      tableBody.innerHTML = `
        <tr>
          <td colspan="4" class="loading-message">
            <div class="loading-spinner"></div>
            <span>Rescanning folder...</span>
          </td>
        </tr>`;
    }
  }

  // Call the API endpoint
  fetch(`/api/watchers/${watcherId}/rescan`, {
    method: 'POST',
  })
    .then((response) => response.json())
    .then((data) => {
      console.log("Rescan response:", data);
      if (data.success) {
        showToast?.('Folder rescan triggered successfully', 'success');
        // Wait a moment for files to be processed, then refresh the list
        setTimeout(() => {
          // Refresh the files list after scan
          if (detailsRow) {
            loadWatcherFiles(watcherId, detailsRow);
          }
        }, 1000);
      } else {
        showToast?.(`Error: ${data.error || 'Failed to rescan folder'}`, 'error');
        // Refresh the file list anyway after an error
        if (detailsRow) {
          loadWatcherFiles(watcherId, detailsRow);
        }
      }
    })
    .catch((error) => {
      console.error('Error triggering rescan:', error);
      showToast?.('Error communicating with server', 'error');
      // Refresh the file list anyway after an error
      if (detailsRow) {
        loadWatcherFiles(watcherId, detailsRow);
      }
    });
};

