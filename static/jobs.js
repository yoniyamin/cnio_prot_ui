/************************************
 * jobs.js
 * All code for loading jobs,
 * displaying them, sorting them, etc.
 ************************************/

import { createProgressBar, formatDate } from './utils.js';

let jobsData = [];
let currentJobSort = { column: 'id', ascending: false };
let currentJobsPage = 1;
let jobsPerPage = 8; // Default to 8 items per page

// Add a function to change page size
export function setJobsPageSize(size) {
  jobsPerPage = parseInt(size, 10) || 8;
  currentJobsPage = 1; // Reset to first page when changing page size
  updateJobsPagination();
  displayJobs(getPaginatedJobs());
}

function getPaginatedJobs() {
  if (!jobsData || !Array.isArray(jobsData)) {
    return [];
  }

  const startIndex = (currentJobsPage - 1) * jobsPerPage;
  const endIndex = startIndex + jobsPerPage;
  return jobsData.slice(startIndex, endIndex);
}

// ====== Fetch jobs from server ======
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

  return fetch('/api/jobs')
    .then(response => response.json())
    .then(data => {
      // Add a safe check for data
      if (!data || !Array.isArray(data.jobs)) {
        console.warn('Invalid jobs data structure:', data);
        // Initialize with an empty array if data is invalid
        return sortAndDisplayJobs('id', false, []);
      }
      return sortAndDisplayJobs('id', false, data.jobs);
    })
    .catch(error => {
      console.error('Error loading jobs:', error);
      if (tbody) {
        tbody.innerHTML = `
          <tr>
            <td colspan="7" class="error-message">
              Failed to load jobs. Please try again.
            </td>
          </tr>
        `;
      }
      // Return empty array to prevent further errors
      return [];
    });
}

// ====== Sort jobs (table header clicks, etc.) ======
function sortAndDisplayJobs(sortBy = 'id', ascending = false, jobsData = null) {
  // If jobsData is provided, use it; otherwise, use the module-level jobs variable
  // Make sure to handle the case where neither is available
  const jobs = jobsData || window.jobsData || [];
  // Store the sort info
  currentJobSort.column = sortBy;
  currentJobSort.ascending = ascending;

  // Store the sorted jobs back to the module-level variable or window
  window.jobsData = sortJobs(jobs, sortBy, ascending);
  // Reset to first page when sorting
  currentJobsPage = 1;
  // Display the sorted jobs
  displayJobs(window.jobsData);

  // Return the sorted jobs for chaining
  return window.jobsData;
}

// Function to sort jobs
function sortJobs(jobs, sortBy, ascending) {
  // Add null check to prevent error
  if (!jobs || !Array.isArray(jobs)) {
    console.warn('Invalid jobs data:', jobs);
    return [];
  }

  return [...jobs].sort((a, b) => {
    // Add null checks to handle missing properties
    if (!a || !b) return 0;

    let valA = a[sortBy];
    let valB = b[sortBy];

    // Handle missing values
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

  // Add null check to prevent error
  if (!jobs || jobs.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7; // Update to match your actual column count
    cell.textContent = 'No jobs found.';
    cell.className = 'empty-table-message';
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }

  jobs.forEach(job => {
    // Skip if job is null or undefined
    if (!job) return;

    const row = document.createElement('tr');
    row.dataset.jobId = job.id;

    // ID column
    const idCell = document.createElement('td');
    idCell.className = 'col-id';
    idCell.textContent = job.id;
    row.appendChild(idCell);

    // Name column
    const nameCell = document.createElement('td');
    nameCell.className = 'col-name';
    nameCell.textContent = job.name || `Job ${job.id}`;
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
    typeCell.textContent = job.type || 'Unknown';
    row.appendChild(typeCell);

    // Progress column with new progress bar
    const progressCell = document.createElement('td');
    progressCell.className = 'col-progress';

    // Get progress info for the progress bar
    const percentComplete = job.progress_percent || 0;
    const stepsComplete = job.steps_completed || 0;
    const totalSteps = job.total_steps || 0;

    // Create label with step counts if available
    let progressLabel;
    if (totalSteps > 0) {
      progressLabel = `${stepsComplete}/${totalSteps} steps`;
    } else {
      progressLabel = `${percentComplete}%`;
    }

    // Add the progress bar to the cell
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
    detailsButton.className = 'action-icon info';
    detailsButton.title = 'View job details';
    detailsButton.innerHTML = '<i class="fas fa-info-circle"></i>';
    detailsButton.addEventListener('click', () => viewJobDetails(job.id));

    // Cancel job button (if running)
    const cancelButton = document.createElement('button');
    cancelButton.className = 'action-icon terminate';
    cancelButton.title = 'Cancel job';
    cancelButton.innerHTML = '<i class="fas fa-stop-circle"></i>';
    cancelButton.addEventListener('click', () => cancelJob(job.id));

    // Only show cancel button if the job is active
    if (['running', 'pending', 'queued'].includes(status)) {
      actionsDiv.appendChild(cancelButton);
    }

    actionsDiv.appendChild(detailsButton);
    actionsCell.appendChild(actionsDiv);
    row.appendChild(actionsCell);

    tbody.appendChild(row);
  });
  updateJobsPagination();
}

function updateJobsPagination() {
  const totalPages = Math.ceil((jobsData?.length || 0) / jobsPerPage);

  // Update page info
  const currentPageEl = document.getElementById('jobs-current-page');
  const totalPagesEl = document.getElementById('jobs-total-pages');
  const pageSizeEl = document.getElementById('jobs-page-size');

  if (currentPageEl) currentPageEl.textContent = currentJobsPage;
  if (totalPagesEl) totalPagesEl.textContent = totalPages;
  if (pageSizeEl) pageSizeEl.value = jobsPerPage.toString();

  // Enable/disable pagination buttons
  const prevBtn = document.getElementById('jobs-prev-page');
  const nextBtn = document.getElementById('jobs-next-page');

  if (prevBtn) prevBtn.disabled = currentJobsPage <= 1;
  if (nextBtn) nextBtn.disabled = currentJobsPage >= totalPages;
}

// Add pagination helper functions
function prevJobsPage() {
  if (currentJobsPage > 1) {
    currentJobsPage--;
    displayJobs(getPaginatedJobs());
  }
}

function nextJobsPage() {
  const totalPages = Math.ceil((jobsData?.length || 0) / jobsPerPage);
  if (currentJobsPage < totalPages) {
    currentJobsPage++;
    displayJobs(getPaginatedJobs());
  }
}

// ====== Stop job logic ======
function stopJob(jobId) {
  fetch(`/api/jobs/${jobId}/stop`, {
    method: 'POST',
  })
    .then((res) => res.json())
    .then((data) => {
      if (data.success) {
        showToast?.(`Job #${jobId} stopped successfully`, 'success');
        // Refresh jobs after stopping
        loadJobs(false);
      } else {
        showToast?.(`Error stopping job: ${data.error || 'Unknown error'}`, 'error');
      }
    })
    .catch((error) => {
      console.error('Error stopping job:', error);
      showToast?.('Failed to stop job', 'error');
    });
}


export {
  displayJobs,
  sortAndDisplayJobs,
  loadJobs,
  prevJobsPage,
  nextJobsPage
}