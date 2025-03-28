/************************************
 * jobs.js
 * All code for loading jobs,
 * displaying them, sorting them, etc.
 ************************************/

let jobsData = [];
let currentJobSort = { column: 'id', ascending: false };

// ====== Fetch jobs from server ======
export function loadJobs(showLoading = true) {
  const tableBody = document.querySelector('#jobs-table tbody');
  if (!tableBody) return;

  if (showLoading) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="7" class="loading-message">
          <div class="loading-spinner"></div>
          <span>Loading jobs...</span>
        </td>
      </tr>
    `;
  }

  fetch('/api/jobs')
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        tableBody.innerHTML = `
          <tr>
            <td colspan="7" class="loading-message text-red-500">
              Error: ${data.error}
            </td>
          </tr>`;
        return;
      }

      jobsData = data.jobs || [];
      sortAndDisplayJobs();
    })
    .catch((error) => {
      console.error('Error loading jobs:', error);
      tableBody.innerHTML = `
        <tr>
          <td colspan="7" class="loading-message text-red-500">
            Failed to load jobs
          </td>
        </tr>`;
    });
}

// ====== Sort jobs (table header clicks, etc.) ======
export function sortAndDisplayJobs(sortBy = null) {
  if (sortBy) {
    if (sortBy === currentJobSort.column) {
      // Toggle direction
      currentJobSort.ascending = !currentJobSort.ascending;
    } else {
      currentJobSort.column = sortBy;
      currentJobSort.ascending = true;
    }
  }

  // Actually sort
  jobsData.sort((a, b) => {
    let aValue, bValue;
    switch (currentJobSort.column) {
      case 'id':
        aValue = a.id;
        bValue = b.id;
        break;
      case 'name':
        aValue = a.name;
        bValue = b.name;
        break;
      case 'status':
        aValue = a.status;
        bValue = b.status;
        break;
      case 'type':
        aValue = a.job_type || '';
        bValue = b.job_type || '';
        break;
      case 'progress':
        aValue = a.progress || 0;
        bValue = b.progress || 0;
        break;
      case 'creation_time':
        aValue = new Date(a.creation_time);
        bValue = new Date(b.creation_time);
        break;
      default:
        aValue = a.id;
        bValue = b.id;
    }

    if (aValue < bValue) return currentJobSort.ascending ? -1 : 1;
    if (aValue > bValue) return currentJobSort.ascending ? 1 : -1;
    return 0;
  });

  displayJobs();
}

// ====== Render the jobs table ======
function displayJobs() {
  const tableBody = document.querySelector('#jobs-table tbody');
  if (!tableBody) return;

  if (!jobsData.length) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="7" class="loading-message">
          <span>No jobs found</span>
        </td>
      </tr>`;
    return;
  }

  tableBody.innerHTML = '';
  jobsData.forEach((job) => {
    const row = createJobRow(job);
    tableBody.appendChild(row);
  });
}

// ====== Create a single job row ======
function createJobRow(job) {
  const row = document.createElement('tr');
  row.className = 'hover:bg-hover';

  const creationDate = job.creation_time ? new Date(job.creation_time).toLocaleString() : 'N/A';
  const jobProgress = job.progress ? `${Math.round(job.progress)}%` : '0%';

  // If you have new UI or old UI differences, handle them here
  row.innerHTML = `
    <td>${job.id}</td>
    <td>${job.name}</td>
    <td><span class="status-badge ${job.status}">${job.status}</span></td>
    <td>${job.job_type}</td>
    <td>${jobProgress}</td>
    <td>${creationDate}</td>
    <td>
      <!-- Example action icons or buttons -->
      <button class="action-icon terminate" title="Stop Job" data-jobid="${job.id}">
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5"
             viewBox="0 0 20 20" fill="currentColor">
          <path fill-rule="evenodd" 
                d="M6 18L18 6M6 6l12 12" 
                clip-rule="evenodd" />
        </svg>
      </button>
    </td>
  `;

  // Example: hooking up stop action
  const stopBtn = row.querySelector('.terminate');
  stopBtn.addEventListener('click', () => stopJob(job.id));

  return row;
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
