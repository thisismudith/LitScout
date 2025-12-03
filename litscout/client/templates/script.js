(function () {
  // Theme switching
  const themeButtons = document.querySelectorAll(".theme-btn");
  themeButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const theme = btn.dataset.theme || "desktop";
      document.documentElement.setAttribute("data-theme", theme);
    });
  });

  // Mode toggle: show/hide upload form vs query input state
  const modeRadios = document.querySelectorAll('input[name="mode"]');
  const uploadForm = document.getElementById("upload-form");
  const searchForm = document.querySelector(".search-form");

  function syncMode() {
    const selected = document.querySelector('input[name="mode"]:checked');
    if (!selected) return;
    const mode = selected.value;

    if (mode === "upload") {
      if (searchForm) {
        const qInput = searchForm.querySelector('input[name="q"]');
        if (qInput) qInput.disabled = true;
      }
      if (uploadForm) uploadForm.style.display = "flex";
    } else {
      if (searchForm) {
        const qInput = searchForm.querySelector('input[name="q"]');
        if (qInput) qInput.disabled = false;
      }
      if (uploadForm) uploadForm.style.display = "none";
    }
  }

  modeRadios.forEach((r) => r.addEventListener("change", syncMode));
  syncMode();

  // Submit on Enter in search input
  const searchInput = document.querySelector('.search-input');
  if (searchInput && searchForm) {
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        searchForm.submit();
      }
    });
  }
})();
