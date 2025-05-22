// Function to create a progress bar element
function createProgressBar(value, label) {
  // Ensure value is between 0-100
  const safeValue = Math.min(Math.max(parseInt(value) || 0, 0), 100);

  // Create the progress bar container
  const container = document.createElement('div');
  container.className = 'progress-bar-container';

  // Create the fill element - no text inside the fill
  const fill = document.createElement('div');
  fill.className = 'progress-bar-fill';
  fill.style.width = `${safeValue}%`;

  // Add a separate label element below the bar
  const labelElement = document.createElement('div');
  labelElement.className = 'progress-label';
  labelElement.textContent = label || `${safeValue}%`;

  // Add elements to container
  container.appendChild(fill);
  container.appendChild(labelElement);

  return container;
}

function formatDate(dateString) {
  if (!dateString) return 'N/A';

  try {
    const date = new Date(dateString);
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch (error) {
    console.error('Error formatting date:', error);
    return dateString;
  }
}
function fetchWithLogging(url, options = {}) {
  console.group(`🔄 API Request: ${url}`);
  console.log('Request Options:', options);
  console.time('Request Duration');

  return fetch(url, options)
    .then(response => {
      console.log(`Response Status: ${response.status} ${response.statusText}`);

      // Clone the response to read it twice
      const clonedResponse = response.clone();

      // First return the original promise chain
      return response.json()
        .then(data => {
          console.log('Response Data:', data);
          console.timeEnd('Request Duration');
          console.groupEnd();
          return data;
        })
        .catch(error => {
          // If parsing as JSON fails, try to get text
          console.warn('Failed to parse as JSON, trying text:', error);
          return clonedResponse.text()
            .then(text => {
              console.log('Response as text:', text);
              console.timeEnd('Request Duration');
              console.groupEnd();
              // Try to parse as JSON one more time, in case the error was transient
              try {
                return JSON.parse(text);
              } catch (e) {
                // If it still fails, throw the original error
                throw error;
              }
            });
        });
    })
    .catch(error => {
      console.error('Fetch Error:', error);
      console.timeEnd('Request Duration');
      console.groupEnd();
      throw error;
    });
}

function verifyWatcherFunctionality(watcherId) {
  console.log(`Verifying watcher functionality for ID: ${watcherId}`);

  return fetchWithLogging(`/api/watchers/${watcherId}`)
    .then(data => {
      // Check for expected watcher properties
      const missingProps = [];
      ['id', 'status', 'folder_path', 'file_pattern'].forEach(prop => {
        if (!(prop in data)) {
          missingProps.push(prop);
        }
      });

      if (missingProps.length > 0) {
        console.warn(`Watcher is missing expected properties: ${missingProps.join(', ')}`);
      }

      // Check status
      if (data.status === 'monitoring') {
        console.log("Watcher is active and monitoring");
      } else {
        console.warn(`Watcher is not in monitoring state, current status: ${data.status}`);
      }

      return data;
    });
}


// Function to check if the backend is properly functioning
function checkBackendHealth() {
  console.log("Checking backend health...");

  // Basic fetch to a simple endpoint to test connectivity
  return fetch('/api/health')
    .then(response => {
      if (!response.ok) {
        console.warn("Backend health check returned non-OK status:", response.status);
        return { status: 'warning', message: 'Backend returned a non-OK status' };
      }
      return response.json();
    })
    .then(data => {
      console.log("Backend health check result:", data);
      return data;
    })
    .catch(error => {
      console.error("Backend health check failed:", error);
      return { status: 'error', message: error.message };
    });
}



export { createProgressBar, formatDate, fetchWithLogging, checkBackendHealth, verifyWatcherFunctionality };