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
  showToast?.(`Job details feature coming soon`, 'info');
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