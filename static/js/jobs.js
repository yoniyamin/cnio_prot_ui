/************************************
 * jobs.js
 * All code for loading jobs,
 * displaying them, sorting them, etc.
 ************************************/

import { createProgressBar, formatDate } from './utils.js';

// Make sure we use the same reference for jobs data throughout the module
let currentJobSort = { column: 'id', ascending: false };
let currentJobsPage = 1;
let jobsPerPage = 8; // Default to 8 items per page
// Create a variable to track if we've already highlighted this job
let highlightedJobId = null;
let highlightTimeout = null;


// Add a function to change page size
export function setJobsPageSize(size) {
  jobsPerPage = parseInt(size, 10) || 8;
  currentJobsPage = 1; // Reset to first page when changing page size
  updateJobsPagination();
  displayJobs(getPaginatedJobs());
  highlightSelectedJob(); // Re-apply highlighting after changing pages
}

function getPaginatedJobs() {
  const jobs = window.jobsData || [];
  if (!jobs || !Array.isArray(jobs)) {
    return [];
  }
  const startIndex = (currentJobsPage - 1) * jobsPerPage;
  const endIndex = startIndex + jobsPerPage;
  return jobs.slice(startIndex, endIndex);
}

function loadJobs(showLoading = true) {
  const tbody = document.querySelector('#jobs-table tbody');

  if (showLoading && tbody) {
    tbody.innerHTML = `
      <tr>
        <td colspan="7" class="loading-message">
          <div class="loading-spinner"></div>
          <span>Loading jobs...</span>
        </td>
      </tr>
    `;
  }

  // Use the jobs data passed from the Flask template
  const jobs = window.jobsData || [];
  console.log('Loaded jobs:', jobs); // Debug log to confirm data

  // Check for job_id in the URL to navigate to the correct page
  const urlParams = new URLSearchParams(window.location.search);
  const jobId = urlParams.get('job_id');
  if (jobId) {
    // Find the index of the job in the sorted data
    const jobIndex = jobs.findIndex(job => String(job.job_id) === String(jobId));
    if (jobIndex !== -1) {
      // Calculate the page number (1-based)
      const page = Math.floor(jobIndex / jobsPerPage) + 1;
      currentJobsPage = page;
      console.log(`Found job #${jobId} at index ${jobIndex}, setting page to ${page}`);
    } else {
      console.warn(`Job ID ${jobId} not found in jobs data`);
    }
  }

  return sortAndDisplayJobs('id', false, jobs);
}

// ====== Sort jobs (table header clicks, etc.) ======
function sortAndDisplayJobs(sortBy = 'id', ascending = false, jobsData = null) {
  // If jobsData is provided, use it; otherwise, use the window.jobsData
  const jobs = jobsData || window.jobsData || [];
  // Store the sort info
  currentJobSort.column = sortBy;
  currentJobSort.ascending = ascending;

  // Store the sorted jobs back to the window variable
  window.jobsData = sortJobs(jobs, sortBy, ascending);

  // Display the sorted jobs
  displayJobs(getPaginatedJobs());

  // Re-apply highlighting after sorting
  highlightSelectedJob();

  // Return the sorted jobs for chaining
  return window.jobsData;
}

// Function to sort jobs
function sortJobs(jobs, sortBy, ascending) {
  if (!jobs || !Array.isArray(jobs)) {
    console.warn('Invalid jobs data:', jobs);
    return [];
  }

  return [...jobs].sort((a, b) => {
    if (!a || !b) return 0;

    let valA = a[sortBy];
    let valB = b[sortBy];

    if (valA === undefined) valA = '';
    if (valB === undefined) valB = '';

    if (typeof valA === 'string' && typeof valB === 'string') {
      return ascending ? valA.localeCompare(valB) : valB.localeCompare(valA);
    } else {
      return ascending ? valA - valB : valB - valA;
    }
  });
}

// ====== Render the jobs table ======
function displayJobs(jobs) {
  const tbody = document.querySelector('#jobs-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  if (!jobs || jobs.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7;
    cell.textContent = 'No jobs found.';
    cell.className = 'empty-table-message';
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  jobs.forEach(job => {
    if (!job) return;

    const row = document.createElement('tr');
    row.className = 'job-row';
    row.dataset.jobId = job.job_id || job.id;

    // ID column
    const idCell = document.createElement('td');
    idCell.className = 'col-id';
    idCell.textContent = job.job_id || job.id;
    row.appendChild(idCell);

    // Name column
    const nameCell = document.createElement('td');
    nameCell.className = 'col-name';
    nameCell.textContent = job.job_name || job.name || `Job ${job.job_id || job.id}`;
    row.appendChild(nameCell);

    // Status column
    const statusCell = document.createElement('td');
    statusCell.className = 'col-status';
    const statusBadge = document.createElement('span');
    const status = (job.status || 'unknown').toLowerCase();
    statusBadge.className = `status-badge ${status}`;
    statusBadge.textContent = job.status || 'Unknown';
    statusCell.appendChild(statusBadge);
    row.appendChild(statusCell);

    // Type column
    const typeCell = document.createElement('td');
    typeCell.className = 'col-type';
    typeCell.textContent = job.job_type || job.type || 'Unknown';
    row.appendChild(typeCell);

    // Progress column
    const progressCell = document.createElement('td');
    progressCell.className = 'col-progress';
    const percentComplete = job.progress_percent || job.progress * 100 || 0;
    const progressLabel = `${Math.round(percentComplete)}%`;
    progressCell.appendChild(createProgressBar(percentComplete, progressLabel));
    row.appendChild(progressCell);

    // Created column
    const createdCell = document.createElement('td');
    createdCell.className = 'col-created';
    createdCell.textContent = job.creation_time ? formatDate(job.creation_time) : 'N/A';
    row.appendChild(createdCell);

    // Actions column
    const actionsCell = document.createElement('td');
    actionsCell.className = 'col-actions';
    const actionsDiv = document.createElement('div');
    actionsDiv.className = 'action-icons';

    // Details button
    const detailsButton = document.createElement('button');
    detailsButton.className = 'action-icon info toggle-details';
    detailsButton.title = 'View job details';
    detailsButton.innerHTML = '<i class="fas fa-info-circle"></i>';
    detailsButton.dataset.id = job.job_id || job.id;

    // Cancel job button (if running)
    const cancelButton = document.createElement('button');
    cancelButton.className = 'action-icon terminate stop-job';
    cancelButton.title = 'Cancel job';
    cancelButton.innerHTML = '<i class="fas fa-stop-circle"></i>';
    cancelButton.dataset.id = job.job_id || job.id;

    if (['running', 'waiting', 'queued'].includes(status)) {
      actionsDiv.appendChild(cancelButton);
    }

    actionsDiv.appendChild(detailsButton);
    actionsCell.appendChild(actionsDiv);
    row.appendChild(actionsCell);

    tbody.appendChild(row);
  });

  attachJobEventListeners();
  updateJobsPagination();
}

// Function to highlight the selected job based on URL parameter
function highlightSelectedJob() {
  // Check URL for job_id parameter
  const urlParams = new URLSearchParams(window.location.search);
  const jobId = urlParams.get('job_id');

  // If there's no job_id in the URL, clear any existing highlights
  if (!jobId) {
    document.querySelectorAll('.job-row.highlighted').forEach(row => {
      row.classList.remove('highlighted');
    });
    highlightedJobId = null;
    if (highlightTimeout) {
      clearTimeout(highlightTimeout);
      highlightTimeout = null;
    }
    return;
  }

  // If we've already highlighted this job, don't do it again
  if (jobId === highlightedJobId) {
    return;
  }

  console.log(`Looking for job #${jobId} to highlight`);

  // Clear any existing highlights first
  document.querySelectorAll('.job-row.highlighted').forEach(row => {
    row.classList.remove('highlighted');
  });

  // Clear any existing timeout
  if (highlightTimeout) {
    clearTimeout(highlightTimeout);
  }

  const jobRow = document.querySelector(`.job-row[data-job-id="${jobId}"]`);
  if (jobRow) {
    console.log(`Found job row to highlight:`, jobRow);

    // Set the flag that we've highlighted this job
    highlightedJobId = jobId;

    // Apply the highlight
    jobRow.classList.add('highlighted');

    // Scroll to the highlighted row
    jobRow.scrollIntoView({ behavior: 'smooth', block: 'center' });

    // Remove highlight class after animation completes
    // The animation duration is 5s, so we'll remove the class after 5.1s
    highlightTimeout = setTimeout(() => {
      jobRow.classList.remove('highlighted');

      // Also, automatically remove the job_id from the URL after the highlight fades
      // This prevents re-highlighting when auto-refreshing
      const newUrl = window.location.pathname;
      window.history.replaceState({}, document.title, newUrl);

      highlightedJobId = null;
      highlightTimeout = null;
    }, 5100);
  } else {
    console.warn(`Job row with ID ${jobId} not found in current page`);
  }
}

function attachJobEventListeners() {
  document.querySelectorAll('.toggle-details').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const jobId = btn.dataset.id;
      if (jobId) {
        viewJobDetails(jobId);
      }
    });
  });

  document.querySelectorAll('.stop-job').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const jobId = btn.dataset.id;
      if (jobId) {
        confirmStopJob(jobId);
      }
    });
  });
}

function initJobsIconStyles() {
  const style = document.createElement('style');
  style.textContent = `
    #jobs-table .toggle-details,
    #jobs-table .stop-job {
      width: 1.25rem;
      height: 1.25rem;
      background: none;
      border: none;
      padding: 0;
      margin: 0;
      cursor: pointer;
    }
    #jobs-table .data-table tbody tr:hover .action-icons img {
      background: transparent;
    }
  `;
  document.head.appendChild(style);
  console.log('Job action icons styling applied');
}

function viewJobDetails(jobId) {
  console.log(`Viewing details for job ID: ${jobId}`);

  // Get the modal elements
  const modal = document.getElementById('job-details-modal');
  const closeBtn = document.getElementById('close-job-details-modal');
  const closeFooterBtn = document.getElementById('close-job-details-btn');
  const stopJobBtn = document.getElementById('stop-job-btn');

  if (!modal) {
    console.error("Job details modal not found in the DOM");
    showToast?.("Could not display job details: Modal not found", 'error');
    return;
  }

  // Show loading state
  const detailsContent = document.getElementById('job-details-content');
  const detailsContainer = document.getElementById('job-details-container');
  const detailsError = document.getElementById('job-details-error');

  if (detailsContent) detailsContent.style.display = 'block';
  if (detailsContainer) detailsContainer.style.display = 'none';
  if (detailsError) detailsError.style.display = 'none';

  // Set up tab navigation
  setupTabNavigation();

  // Function to close the modal
  const closeModal = () => {
    modal.classList.remove('active');
  };

  // Add event listeners to close buttons
  if (closeBtn) closeBtn.addEventListener('click', closeModal);
  if (closeFooterBtn) closeFooterBtn.addEventListener('click', closeModal);

  // Stop job button functionality
  if (stopJobBtn) {
    stopJobBtn.addEventListener('click', () => {
      closeModal();
      confirmStopJob(jobId);
    });
  }

  // Add listener for refresh files button
  const refreshFilesBtn = modal.querySelector('.refresh-files');
  if (refreshFilesBtn) {
    refreshFilesBtn.addEventListener('click', () => {
      loadJobFiles(jobId);
    });
  }

  // Show the modal
  modal.classList.add('active');

  // First try to find the job in the existing jobs data
  const job = findJobById(jobId);

  if (job) {
    displayJobDetails(job);
    // Still fetch files separately
    loadJobFiles(jobId);
  } else {
    // If not found in the current data, fetch it from the server
    fetchJobDetails(jobId);
  }
}

// Set up tab navigation
function setupTabNavigation() {
  const tabButtons = document.querySelectorAll('.tab-button');
  const tabContents = document.querySelectorAll('.tab-content');

  tabButtons.forEach(button => {
    button.addEventListener('click', () => {
      // Remove active class from all buttons
      tabButtons.forEach(btn => btn.classList.remove('active'));

      // Hide all tab contents
      tabContents.forEach(content => content.style.display = 'none');

      // Add active class to clicked button
      button.classList.add('active');

      // Show corresponding tab content
      const tabId = button.dataset.tab;
      const tabContent = document.getElementById(`${tabId}-tab`);
      if (tabContent) tabContent.style.display = 'block';
    });
  });
}

// Function to load files associated with a job
function loadJobFiles(jobId) {
  console.log(`Loading files for job ID: ${jobId}`);

  const tableBody = document.getElementById('job-files-tbody');
  if (!tableBody) {
    console.error("Files table body not found");
    return;
  }

  // Show loading message
  tableBody.innerHTML = `
    <tr>
      <td colspan="4" class="loading-message">
        <div class="loading-spinner"></div>
        <span>Loading files...</span>
      </td>
    </tr>`;

  // Clear any existing fetch requests to prevent race conditions
  if (window.currentFileFetch) {
    window.currentFileFetch.aborted = true;
  }

  // Store the fetch controller to allow potential cancellation
  const controller = new AbortController();
  const signal = controller.signal;

  window.currentFileFetch = {
    controller,
    jobId,
    aborted: false
  };

  fetch(`/api/jobs/${jobId}/files`, { signal })
    .then((response) => {
      console.log(`Files API response status: ${response.status}`);
      if (window.currentFileFetch.aborted) {
        throw new Error('Fetch aborted due to new request');
      }

      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      if (window.currentFileFetch.aborted) {
        throw new Error('Fetch aborted due to new request');
      }

      console.log("Files API full response:", data);

      if (data.error) {
        tableBody.innerHTML = `
          <tr><td colspan="4" class="loading-message">
            <span class="text-red-500">Error: ${data.error}</span>
          </td></tr>`;
        return;
      }

      // Handle different possible response structures
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
          files = [];
        }
      }

      // Filter out any null or undefined entries
      files = files.filter(file => file);

      console.log(`Found ${files.length} files`, files);

      // Debug empty files array
      if (!files || files.length === 0) {
        console.log("No files found in response", data);
        tableBody.innerHTML = `
          <tr><td colspan="4" class="loading-message">
            <span>No files associated with this job</span>
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
        initFileTableSort(files);

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
      if (window.currentFileFetch.aborted) {
        console.log("File fetch was aborted, not showing error");
        return;
      }

      console.error('Error loading files:', err);
      tableBody.innerHTML = `
        <tr><td colspan="4" class="loading-message">
          <span class="text-red-500">Failed to load files: ${err.message}</span>
          <button class="action-button secondary retry-button">Retry</button>
        </td></tr>`;

      // Add retry functionality
      const retryButton = tableBody.querySelector('.retry-button');
      if (retryButton) {
        retryButton.addEventListener('click', () => loadJobFiles(jobId));
      }
    })
    .finally(() => {
      // Clean up request reference if it's still the current one
      if (window.currentFileFetch && window.currentFileFetch.jobId === jobId && !window.currentFileFetch.aborted) {
        window.currentFileFetch = null;
      }
    });
}

// Initialize file table sorting
function initFileTableSort(filesArray) {
  const headers = document.querySelectorAll('.files-table th[data-sort]');
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
      const tbody = document.getElementById('job-files-tbody');
      if (tbody) {
        tbody.innerHTML = '';
        filesArray.forEach((file) => {
          tbody.appendChild(createFileRow(file));
        });
      }
    });
  });
}

// Compare files for sorting
function compareFiles(a, b, column, ascending) {
  let aValue, bValue;

  switch (column) {
    case 'file_name':
      aValue = a.file_name || a.name || a.filename || '';
      bValue = b.file_name || b.name || b.filename || '';
      break;
    case 'file_path':
      aValue = a.file_path || a.path || '';
      bValue = b.file_path || b.path || '';
      break;
    case 'capture_time':
      aValue = a.capture_time ? new Date(a.capture_time) : new Date(0);
      bValue = b.capture_time ? new Date(b.capture_time) : new Date(0);
      break;
    case 'job_id':
      aValue = a.job_id || a.jobId || '';
      bValue = b.job_id || b.jobId || '';
      break;
    case 'status':
      aValue = a.status || a.state || '';
      bValue = b.status || b.state || '';
      break;
    default:
      aValue = a.file_name || a.name || '';
      bValue = b.file_name || b.name || '';
  }

  if (aValue < bValue) return ascending ? -1 : 1;
  if (aValue > bValue) return ascending ? 1 : -1;
  return 0;
}

// Helper function to find a job by ID in the window.jobsData array
function findJobById(jobId) {
  if (!window.jobsData || !Array.isArray(window.jobsData)) {
    return null;
  }

  // Convert to string for safer comparison
  const searchId = String(jobId);

  return window.jobsData.find(job =>
    String(job.job_id) === searchId || String(job.id) === searchId
  );
}

function createFileRow(file) {
  console.log("Creating row for file:", file);

  const row = document.createElement('tr');
  row.className = 'hover:bg-hover';

  // Handle different possible file object structures
  const fileName = file.file_name || file.name || file.filename ||
                  (typeof file === 'string' ? file : 'Unknown');

  const filePath = file.file_path || file.path || file.fullPath || 'N/A';

  // Default to "captured" status instead of "pending" if no status is found
  const fileStatus = file.status || file.state || 'captured';

  // Status cell (first column)
  const statusCell = document.createElement('td');
  const statusBadge = document.createElement('span');
  statusBadge.className = `status-badge ${fileStatus}`;
  statusBadge.textContent = fileStatus;
  statusCell.appendChild(statusBadge);
  row.appendChild(statusCell);

  // File name cell (second column)
  const nameCell = document.createElement('td');
  nameCell.title = fileName;
  nameCell.textContent = fileName;
  row.appendChild(nameCell);

  // File path cell (third column)
  const pathCell = document.createElement('td');
  pathCell.title = filePath;
  pathCell.textContent = filePath;
  row.appendChild(pathCell);

  return row;
}

// Function to fetch job details from the API
function fetchJobDetails(jobId) {
  console.log(`Fetching details for job ID: ${jobId}`);

  // Display loading state
  const detailsContent = document.getElementById('job-details-content');
  const detailsContainer = document.getElementById('job-details-container');
  const detailsError = document.getElementById('job-details-error');

  if (detailsContent) detailsContent.style.display = 'block';
  if (detailsContainer) detailsContainer.style.display = 'none';
  if (detailsError) detailsError.style.display = 'none';

  // Call the job API to get details
  fetch(`/api/jobs/${jobId}`)
    .then(response => {
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      return response.json();
    })
    .then(data => {
      // Check if we got valid job data
      if (data && (data.job || (data.jobs && data.jobs.length > 0))) {
        const jobData = data.job || data.jobs[0];
        displayJobDetails(jobData);

        // Also fetch files for this job
        loadJobFiles(jobId);
      } else {
        throw new Error('No job data returned from API');
      }
    })
    .catch(error => {
      console.error('Error fetching job details:', error);

      // Show error message
      if (detailsContent) detailsContent.style.display = 'none';
      if (detailsError) {
        detailsError.style.display = 'flex';
        detailsError.querySelector('span').textContent = `Failed to load job details: ${error.message}`;
      }
    });
}

// Function to display job details in the modal
// Updated displayJobDetails function to handle SQLite row object
function displayJobDetails(job) {
  console.log('Displaying job details:', job);

  // Hide loading, show details container
  const detailsContent = document.getElementById('job-details-content');
  const detailsContainer = document.getElementById('job-details-container');
  const detailsError = document.getElementById('job-details-error');

  if (detailsContent) detailsContent.style.display = 'none';
  if (detailsError) detailsError.style.display = 'none';
  if (detailsContainer) detailsContainer.style.display = 'block';

  // Handle both object and array formats from SQLite
  // SQLite returns rows as arrays when using fetchone() without row_factory
  const isArrayFormat = Array.isArray(job);

  // Extract fields based on format
  const jobId = isArrayFormat ? job[1] : (job.job_id || job.id || 'N/A');
  const jobName = isArrayFormat ? job[2] : (job.job_name || job.name || 'Unnamed Job');
  const jobType = isArrayFormat ? job[5] : (job.job_type || job.type || 'N/A');
  const jobStatus = isArrayFormat ? job[8] : (job.status || 'Unknown');
  const creationTime = isArrayFormat ? job[9] : job.creation_time;
  const completionTime = isArrayFormat ? job[10] : job.completion_time;
  const watcherId = isArrayFormat ? job[7] : (job.watcher_id || (job.extras_dict && job.extras_dict.watcher_id));
  const jobDemands = isArrayFormat ? job[4] : (job.job_demands || job.demands || 'N/A');

  // Fill in the basic info tab
  document.getElementById('job-id').textContent = jobId;
  document.getElementById('job-name').textContent = jobName;

  // Status with badge
  const statusCell = document.getElementById('job-status');
  if (statusCell) {
    const status = (jobStatus || 'unknown').toLowerCase();
    statusCell.innerHTML = `<span class="status-badge ${status}">${jobStatus}</span>`;
  }

  document.getElementById('job-type').textContent = jobType;

  // Progress with progress bar
  const progressCell = document.getElementById('job-progress');
  if (progressCell) {
    // For SQLite format we don't typically store progress, so default to 0 or 100 based on status
    let percentComplete = 0;
    if (isArrayFormat) {
      percentComplete = (jobStatus === 'completed') ? 100 : 0;
    } else {
      percentComplete = job.progress_percent || (job.progress ? job.progress * 100 : 0);
    }

    const progressLabel = `${Math.round(percentComplete)}%`;

    // Create a div for the progress display
    const progressDisplay = document.createElement('div');
    progressDisplay.className = 'progress-display';
    progressDisplay.appendChild(createProgressBar(percentComplete, progressLabel));

    progressCell.innerHTML = '';
    progressCell.appendChild(progressDisplay);
  }

  // Format dates
  document.getElementById('job-created').textContent = creationTime ? formatDate(creationTime) : 'N/A';
  document.getElementById('job-started').textContent = (isArrayFormat ? 'N/A' : formatDate(job.start_time || ''));
  document.getElementById('job-completed').textContent = completionTime ? formatDate(completionTime) : 'N/A';

  // Watcher ID with link if available
  const watcherIdCell = document.getElementById('job-watcher-id');
  if (watcherIdCell) {
    if (watcherId && watcherId !== 'N/A') {
      watcherIdCell.innerHTML = `<a href="/watchers?highlight=${watcherId}" class="watcher-link">${watcherId}</a>`;
    } else {
      watcherIdCell.textContent = 'N/A';
    }
  }

  document.getElementById('job-demands').textContent = jobDemands || 'N/A';

  // Display parameters - pass the full job object for parameter extraction
  displayJobParameters(job, isArrayFormat);

  // Show/hide stop button based on job status
  const stopJobBtn = document.getElementById('stop-job-btn');
  if (stopJobBtn) {
    const status = (jobStatus || '').toLowerCase();
    const canStop = ['running', 'waiting', 'queued'].includes(status);
    stopJobBtn.style.display = canStop ? 'block' : 'none';
  }
}

// Updated function to display job parameters
function displayJobParameters(job, isArrayFormat = false) {
  console.log("Displaying job parameters:", job);

  const parametersContainer = document.getElementById('job-parameters');
  if (!parametersContainer) {
    console.error("Parameters container not found!");
    return;
  }

  // Clear previous content
  parametersContainer.innerHTML = '';

  // Show loading message
  parametersContainer.innerHTML = `
    <div class="loading-message">
      <div class="loading-spinner"></div>
      <span>Loading job configuration...</span>
    </div>
  `;

  // Extract job ID
  const jobId = isArrayFormat ? job[1] : (job.job_id || job.id);

  if (!jobId) {
    parametersContainer.innerHTML = '<p class="no-data-message">Could not determine job ID.</p>';
    return;
  }

  // Fetch job demands from the API directly
  fetch(`/api/jobs/${jobId}/demands`)
    .then(response => {
      if (!response.ok) {
        throw new Error(`Failed to fetch job configuration: ${response.status}`);
      }
      return response.json();
    })
    .then(data => {
      console.log("Received job demands data:", data);

      if (!data.demands || Object.keys(data.demands).length === 0) {
        parametersContainer.innerHTML = '<p class="no-data-message">No configuration parameters available for this job.</p>';
        return;
      }

      // Clear the loading message
      parametersContainer.innerHTML = '';

      // Create the job configuration section
      const sectionDiv = document.createElement('div');
      sectionDiv.className = 'parameter-section';

      // Section title
      const title = document.createElement('h5');
      title.textContent = 'Job Configuration';
      sectionDiv.appendChild(title);

      // Parameter table
      const table = document.createElement('table');
      table.className = 'parameter-table';

      // Table header
      const thead = document.createElement('thead');
      thead.innerHTML = `
        <tr>
          <th>Parameter</th>
          <th>Value</th>
        </tr>
      `;
      table.appendChild(thead);

      // Table body
      const tbody = document.createElement('tbody');

      // Sort parameters alphabetically
      const sortedParams = Object.keys(data.demands).sort();

      sortedParams.forEach(paramName => {
        const paramValue = data.demands[paramName];
        const row = document.createElement('tr');

        // Parameter name cell with formatting
        const nameCell = document.createElement('td');
        const formattedName = paramName
          .replace(/_/g, ' ')
          .replace(/\b\w/g, l => l.toUpperCase());
        nameCell.textContent = formattedName;
        row.appendChild(nameCell);

        // Parameter value cell
        const valueCell = document.createElement('td');
        valueCell.className = 'parameter-value';

        // Format the value based on its type
        if (typeof paramValue === 'object' && paramValue !== null) {
          try {
            if (Array.isArray(paramValue)) {
              // Format arrays nicely
              valueCell.textContent = paramValue.join(', ');
            } else {
              // Pretty format object with indentation
              const formattedJson = JSON.stringify(paramValue, null, 2);
              valueCell.innerHTML = `<pre>${formattedJson}</pre>`;
            }
          } catch (e) {
            valueCell.textContent = String(paramValue);
          }
        } else {
          valueCell.textContent = String(paramValue);
        }

        row.appendChild(valueCell);
        tbody.appendChild(row);
      });

      table.appendChild(tbody);
      sectionDiv.appendChild(table);
      parametersContainer.appendChild(sectionDiv);
    })
    .catch(error => {
      console.error("Error loading job parameters:", error);
      parametersContainer.innerHTML = `
        <div class="error-message">
          <i class="fas fa-exclamation-circle"></i>
          <span>Failed to load job configuration: ${error.message}</span>
        </div>
      `;
    });
}

// Function to display job files in the modal
function displayJobFiles(files) {
  const filesTableBody = document.getElementById('job-files-tbody');
  if (!filesTableBody) return;

  // Clear the table
  filesTableBody.innerHTML = '';

  if (!files || files.length === 0) {
    filesTableBody.innerHTML = `
      <tr>
        <td colspan="4" class="empty-table-message">No files found for this job.</td>
      </tr>`;
    return;
  }

  // Add each file to the table
  files.forEach(file => {
    const row = document.createElement('tr');

    // File name
    const nameCell = document.createElement('td');
    nameCell.textContent = file.file_name || file.name || 'Unknown';
    nameCell.title = nameCell.textContent;
    row.appendChild(nameCell);

    // File path
    const pathCell = document.createElement('td');
    pathCell.textContent = file.file_path || file.path || 'N/A';
    pathCell.title = pathCell.textContent;
    row.appendChild(pathCell);

    // Capture time
    const timeCell = document.createElement('td');
    timeCell.textContent = file.capture_time ? formatDate(file.capture_time) : 'N/A';
    row.appendChild(timeCell);

    // Status
    const statusCell = document.createElement('td');
    const status = file.status || 'captured';
    const statusBadge = document.createElement('span');
    statusBadge.className = `status-badge ${status}`;
    statusBadge.textContent = status;
    statusCell.appendChild(statusBadge);
    row.appendChild(statusCell);

    filesTableBody.appendChild(row);
  });

  // Add sorting functionality to the table headers
  const headers = document.querySelectorAll('.files-table th[data-sort]');
  headers.forEach(th => {
    th.addEventListener('click', () => {
      const sortBy = th.dataset.sort;
      const isAsc = !th.classList.contains('sort-asc');

      // Clear old sort classes
      headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
      th.classList.add(isAsc ? 'sort-asc' : 'sort-desc');

      // Sort files
      files.sort((a, b) => {
        let valA = a[sortBy];
        let valB = b[sortBy];

        if (valA === undefined) valA = '';
        if (valB === undefined) valB = '';

        if (typeof valA === 'string' && typeof valB === 'string') {
          return isAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
        } else {
          return isAsc ? valA - valB : valB - valA;
        }
      });

      // Re-render the table
      displayJobFiles(files);
    });
  });
}

function confirmStopJob(jobId) {
  const confirmModal = document.getElementById('confirm-modal');
  if (confirmModal) {
    showConfirmationModal(
      `Are you sure you want to terminate job #${jobId}?`,
      () => stopJob(jobId)
    );
  } else {
    if (confirm('Are you sure you want to stop this job?')) {
      stopJob(jobId);
    }
  }
}

function updateJobsPagination() {
  const jobs = window.jobsData || [];
  const totalPages = Math.ceil((jobs.length || 0) / jobsPerPage);

  const currentPageEl = document.getElementById('jobs-current-page');
  const totalPagesEl = document.getElementById('jobs-total-pages');
  const pageSizeEl = document.getElementById('jobs-page-size');

  if (currentPageEl) currentPageEl.textContent = currentJobsPage;
  if (totalPagesEl) totalPagesEl.textContent = totalPages;
  if (pageSizeEl) pageSizeEl.value = jobsPerPage.toString();

  const prevBtn = document.getElementById('jobs-prev-page');
  const nextBtn = document.getElementById('jobs-next-page');

  if (prevBtn) prevBtn.disabled = currentJobsPage <= 1;
  if (nextBtn) nextBtn.disabled = currentJobsPage >= totalPages;
}

function prevJobsPage() {
  if (currentJobsPage > 1) {
    currentJobsPage--;
    displayJobs(getPaginatedJobs());
    highlightSelectedJob(); // Re-apply highlighting after page change
  }
}

function nextJobsPage() {
  const totalPages = Math.ceil((window.jobsData?.length || 0) / jobsPerPage);
  if (currentJobsPage < totalPages) {
    currentJobsPage++;
    displayJobs(getPaginatedJobs());
    highlightSelectedJob(); // Re-apply highlighting after page change
  }
}

function stopJob(jobId) {
  fetch(`/api/jobs/${jobId}/stop`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({})
  })
    .then(response => {
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      return response.json();
    })
    .then(data => {
      if (data.success) {
        showToast?.(`Job #${jobId} stopped successfully`, 'success');
        loadJobs(false);
      } else {
        showToast?.(`Error stopping job: ${data.error || 'Unknown error'}`, 'error');
      }
    })
    .catch(error => {
      console.error('Error stopping job:', error);
      showToast?.(`Failed to stop job: ${error.message}`, 'error');
    });
}

document.addEventListener('DOMContentLoaded', () => {
  if (document.querySelector('#jobs-table')) {
    console.log("Found jobs-table, auto-calling loadJobs()");
    // Ensure window.jobsData is set before calling loadJobs
    window.jobsData = window.jobsData || [];
    loadJobs();
  }

  // Attach pagination event listeners
  const prevBtn = document.getElementById('jobs-prev-page');
  const nextBtn = document.getElementById('jobs-next-page');
  const pageSizeSelect = document.getElementById('jobs-page-size');
  const refreshBtn = document.getElementById('refresh-jobs');

  if (prevBtn) prevBtn.addEventListener('click', prevJobsPage);
  if (nextBtn) nextBtn.addEventListener('click', nextJobsPage);
  if (pageSizeSelect) {
    pageSizeSelect.addEventListener('change', (e) => {
      setJobsPageSize(e.target.value);
    });
  }
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => loadJobs(true));
  }
});

export {
  displayJobs,
  sortAndDisplayJobs,
  loadJobs,
  prevJobsPage,
  nextJobsPage,
  initJobsIconStyles,
  highlightSelectedJob // Export the highlight function
}