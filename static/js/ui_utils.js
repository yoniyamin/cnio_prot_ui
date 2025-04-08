// ui_utils.js
// Global UI utilities for modals, toasts, and shared functionality

// Modal handling
export function initModals() {
  // Open modal function
  window.openModal = function(modalId) {
    const modal = typeof modalId === 'string' ? document.getElementById(modalId) : modalId;
    if (!modal) return;
    modal.classList.add('active');
    document.body.style.overflow = 'hidden';
  };

  // Close modal function
  window.closeModal = function(modalId) {
    const modal = typeof modalId === 'string' ? document.getElementById(modalId) : modalId;
    if (!modal) return;
    modal.classList.remove('active');
    document.body.style.overflow = '';
  };

  // Attach event listeners to modal buttons
  document.querySelectorAll('.modal-close, #cancel-watcher').forEach(btn => {
    btn.addEventListener('click', () => {
      const modal = btn.closest('.modal');
      if (modal) window.closeModal(modal);
    });
  });

  // Create watcher button
  const createWatcherBtn = document.getElementById('create-watcher-btn');
  const createWatcherModal = document.getElementById('create-watcher-modal');

  if (createWatcherBtn && createWatcherModal) {
    createWatcherBtn.addEventListener('click', () => {
      console.log('Opening modal');
      window.openModal(createWatcherModal);
    });
  }

  // Confirmation modal
  window.showConfirmationModal = function(message, confirmCallback) {
    const modal = document.getElementById('confirm-modal');
    if (!modal) return;

    const messageEl = document.getElementById('confirm-message');
    if (messageEl) messageEl.textContent = message;

    const confirmBtn = document.getElementById('confirm-action');
    if (confirmBtn) {
      const originalHandler = confirmBtn.onclick;
      confirmBtn.onclick = () => {
        window.closeModal(modal);
        confirmCallback();
        confirmBtn.onclick = originalHandler;
      };
    }

    document.querySelectorAll('.confirm-cancel').forEach(btn => {
      btn.onclick = () => window.closeModal(modal);
    });

    window.openModal(modal);
  };
}

// Toast notifications
export function initToasts() {
  window.showToast = function(message, type = 'success', duration = 3000) {
    const toast = document.getElementById('toast-notification');
    if (!toast) return;

    const toastContent = toast.querySelector('.toast-content');
    const toastMessage = toast.querySelector('.toast-message');

    toastContent.className = 'toast-content';
    if (type) toastContent.classList.add(type);

    toastMessage.textContent = message;
    toast.classList.add('active');

    const timeout = setTimeout(() => {
      toast.classList.remove('active');
    }, duration);

    const closeBtn = toast.querySelector('.toast-close');
    if (closeBtn) {
      closeBtn.onclick = () => {
        clearTimeout(timeout);
        toast.classList.remove('active');
      };
    }
  };
}

// Action icons styling
export function initActionIcons() {
  const style = document.createElement('style');
  style.textContent = `
    .action-icon, .toggle-details, .stop-watcher, .stop-job {
      width: 1.25rem;
      height: 1.25rem;
      background: none;
      border: none;
      padding: 0;
      margin: 0;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }
    
    .action-icons {
      display: flex;
      gap: 0.5rem;
      justify-content: center;
    }
    
    .action-icon i {
      font-size: 1.25rem;
    }
    
    .action-icon.info i {
      color: var(--accent-clr);
    }
    
    .action-icon.terminate i {
      color: rgb(220, 38, 38);
    }
    
    .action-icon:hover {
      background-color: var(--hover-clr);
      border-radius: 0.25rem;
    }
  `;
  document.head.appendChild(style);
  console.log('Action icons styling initialized');
}

// Initialize all UI components
export function initUI() {
  initModals();
  initToasts();
  initActionIcons();
  console.log('UI components initialized');
}