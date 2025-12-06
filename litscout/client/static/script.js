(function () {
  // ---------------------- Theme switching ---------------------- //

  const themeButtons = document.querySelectorAll(".theme-btn");
  themeButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const theme = btn.dataset.theme || "desktop";
      document.documentElement.setAttribute("data-theme", theme);
    });
  });

  // ---------------------- Global state ---------------------- //

  const state = {
    query: "",
    mode: "query",
    searchType: "hybrid",
    paperWeight: 0.8,
    conceptWeight: 0.2,
    panels: {
      papers: {
        items: [],
        total: null,
        backendOffset: 0,
        backendChunkSize: 10, // fetch 10 at a time
        pageSize: 5,
        currentPage: 1,
        loading: false,
        hasMore: true,
      },
      venues: {
        items: [],
        total: null,
        backendOffset: 0,
        backendChunkSize: 10,
        pageSize: 5,
        currentPage: 1,
        loading: false,
        hasMore: true,
      },
      authors: {
        items: [],
        total: null,
        backendOffset: 0,
        backendChunkSize: 10, 
        pageSize: 5,
        currentPage: 1,
        loading: false,
        hasMore: true,
      },
    },
  };

  // ---------------------- Elements ---------------------- //

  const searchForm = document.getElementById("search-form");
  const uploadForm = document.getElementById("upload-form");
  const searchInput = searchForm?.querySelector('input[name="query"]');
  const searchTypeSelect = searchForm?.querySelector('select[name="search_type"]');
  const paperWeightInput = searchForm?.querySelector('input[name="paper_weight"]');
  const conceptWeightInput = searchForm?.querySelector('input[name="concept_weight"]');
  const modeRadios = document.querySelectorAll('input[name="mode"]');

  const papersListEl = document.getElementById("papers-list");
  const papersEmptyEl = document.getElementById("papers-empty");
  const papersLoaderEl = document.getElementById("papers-loader");
  const papersSubtitleEl = document.getElementById("papers-subtitle");

  const authorsListEl = document.getElementById("authors-list");
  const authorsEmptyEl = document.getElementById("authors-empty");
  const authorsLoaderEl = document.getElementById("authors-loader");

  const venuesListEl = document.getElementById("venues-list");
  const venuesEmptyEl = document.getElementById("venues-empty");
  const venuesLoaderEl = document.getElementById("venues-loader");

  const paginationControls = document.querySelectorAll(".pagination-controls");
  const pageSizeGroups = document.querySelectorAll(".panel-page-size");

  // ---------------------- Hide Loading indicators initially ---------------------- //
  if (papersLoaderEl) papersLoaderEl.hidden = true;
  if (venuesLoaderEl) venuesLoaderEl.hidden = true;
  if (authorsLoaderEl) authorsLoaderEl.hidden = true;

  // ---------------------- Mode sync (query vs upload) ---------------------- //

  function syncMode() {
    const selected = document.querySelector('input[name="mode"]:checked');
    const mode = selected ? selected.value : "query";
    state.mode = mode;

    if (mode === "upload") {
      if (searchInput) searchInput.disabled = true;
      if (uploadForm) uploadForm.style.display = "flex";
    } else {
      if (searchInput) searchInput.disabled = false;
      if (uploadForm) uploadForm.style.display = "none";
    }
  }

  modeRadios.forEach((r) => r.addEventListener("change", syncMode));
  syncMode();

  if (searchInput && searchForm) {
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        searchForm.requestSubmit();
      }
    });
  }

  // ---------------------- Fetch helper ---------------------- //

  async function fetchJSON(url, payload) {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    return await resp.json();
  }

  // ---------------------- PAPERS: render + load-more ---------------------- //

  function setPanelLoading(panel, isLoading) {
    const loader = document.getElementById(`${panel}-loader`);
    if (!loader) return;
    loader.classList.toggle('hidden', !isLoading);
  }

  function renderPapersPanel() {
    const panelState = state.panels.papers;
    const items = panelState.items;
    const pageSize = panelState.pageSize;
    const page = panelState.currentPage;

    if (!state.query) {
      papersListEl.innerHTML = "";
      papersEmptyEl.textContent =
        "Type a query or upload a research paper to see results.";
      papersEmptyEl.hidden = false;
      return;
    }

    papersEmptyEl.hidden = true;
    papersListEl.innerHTML = "";

    const subtitle = state.query
      ? `for “${state.query.slice(0, 80)}${state.query.length > 80 ? "…" : ""}”`
      : "";
    papersSubtitleEl.textContent = subtitle;

    const startIdx = (page - 1) * pageSize;
    const endIdx = startIdx + pageSize;
    const pageItems = items.slice(startIdx, endIdx);

    if (!pageItems.length && !panelState.loading && !items.length) {
      papersEmptyEl.textContent = "No papers found for this query.";
      papersEmptyEl.hidden = false;
      return;
    }

    for (const p of pageItems) {
      const li = document.createElement("li");
      li.className = "paper-card";

      const header = document.createElement("div");
      header.className = "paper-header-line";

      const titleEl = document.createElement("h3");
      titleEl.className = "paper-title";
      titleEl.textContent = p.title || `Paper #${p.paper_id}`;

      const scoreEl = document.createElement("span");
      scoreEl.className = "match-label";
      const scoreVal = typeof p.score === "number" ? p.score : 0;
      const pct = (scoreVal * 100).toFixed(2);
      scoreEl.textContent = `${pct}% Match`;

      header.appendChild(titleEl);
      header.appendChild(scoreEl);
      li.appendChild(header);

      if (p.abstract) {
        const absEl = document.createElement("p");
        absEl.className = "paper-abstract";
        const text =
          p.abstract.length > 450 ? p.abstract.slice(0, 450) + "…" : p.abstract;
        absEl.textContent = text;
        li.appendChild(absEl);
      }

      const meta = document.createElement("div");
      meta.className = "paper-meta";

      const ext = p.external_ids || {};
      const openalexId =
        typeof ext === "object" ? ext.openalex || ext["openalex"] : null;
      if (openalexId) {
        const link = document.createElement("a");
        link.className = "text-link";
        link.href = `https://openalex.org/${openalexId}`;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.innerHTML =
          '<span class="icon">🌐</span> View on OpenAlex';
        meta.appendChild(link);
      }

      li.appendChild(meta);
      papersListEl.appendChild(li);
    }

    updatePaginationControls("papers");
  }

  async function loadMorePapersIfNeeded(showLoader = false) {
    const panelState = state.panels.papers;
    if (!panelState.hasMore || panelState.loading) return;

    panelState.loading = true;
    if (showLoader && papersLoaderEl) papersLoaderEl.hidden = false;

    try {
      const res = await fetchJSON("/api/search/papers", {
        query: state.query,
        search_type: state.searchType,
        limit: panelState.backendChunkSize,
        offset: panelState.backendOffset,
        paper_weight: state.paperWeight,
        concept_weight: state.conceptWeight,
      });

      const newItems = res.papers || [];
      const totalFromApi = res.total_papers;
      if (typeof totalFromApi === "number") {
        panelState.total = totalFromApi;
      }

      if (newItems.length < panelState.backendChunkSize) {
        panelState.hasMore = false;
      }

      panelState.items = panelState.items.concat(newItems);
      panelState.backendOffset += newItems.length;
    } catch (err) {
      console.error("Error fetching more papers:", err);
      panelState.hasMore = false;
    } finally {
      panelState.loading = false;
      if (showLoader && papersLoaderEl) papersLoaderEl.hidden = true;
    }
  }

  // ---------------------- AUTHORS: derived from papers ---------------------- //


  function renderAuthorsPanel() {
    const panelState = state.panels.authors;
    const items = panelState.items;
    const pageSize = panelState.pageSize;
    const page = panelState.currentPage;

    authorsListEl.innerHTML = "";
    authorsEmptyEl.hidden = true;

    if (!state.query) {
      authorsEmptyEl.textContent = "Run a search to see prominent authors.";
      authorsEmptyEl.hidden = false;
      return;
    }

    if (!items.length) {
      authorsEmptyEl.textContent =
        "No author information found for these results.";
      authorsEmptyEl.hidden = false;
      return;
    }

    const startIdx = (page - 1) * pageSize;
    const endIdx = startIdx + pageSize;
    const pageItems = items.slice(startIdx, endIdx);

    for (const a of pageItems) {
      const li = document.createElement("li");
      li.className = "author-row";

      const nameEl = document.createElement("span");
      nameEl.className = "author-name";
      nameEl.textContent = a.full_name;
      li.appendChild(nameEl);

      const meta = document.createElement("div");
      meta.className = "author-meta";

      const countEl = document.createElement("span");
      countEl.className = "pill";
      countEl.textContent = `Score: ${(a.score*100).toFixed(2)}`;


      meta.appendChild(countEl);

      const openalexId = a.openalex_url;
      if (openalexId) {
        const link = document.createElement("a");
        link.className = "text-link";
        link.href = openalexId;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.innerHTML =
          '<span class="icon">🌐</span> View on OpenAlex';
        meta.appendChild(link);
      }
      li.appendChild(meta);
      authorsListEl.appendChild(li);
    }

    updatePaginationControls("authors");
  }

  // ---------------------- Upload Paper Handling ---------------------- //
  const uploadBtn = document.getElementById('upload-btn');
  const uploadInput = document.getElementById('upload-input');
  const queryInput = document.getElementById('query-input');
  const searchBtn = document.getElementById('search-btn');

  uploadBtn.addEventListener('click', () => {
    if (!uploadInput) return;
    uploadInput.click();
  });

  uploadInput.addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    try {
      searchBtn.disabled = true;
      searchBtn.textContent = "Processing…";

      const formData = new FormData();
      formData.append('file', file);

      const resp = await fetch('/api/upload_query', {
        method: 'POST',
        body: formData,
      });
      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        console.error(data.error || "Upload failed");
        return;
      }

      queryInput.value = data.query || "";

      triggerSearch();
    } catch (err) {
      console.error('Upload error', err);
    } finally {
      searchBtn.disabled = false;
      searchBtn.textContent = "Search";
      // Reset input so same file can be chosen again if needed
      uploadInput.value = "";
    }
  });

  // ---------------------- VENUES: render + load-more ---------------------- //

  function renderVenuesPanel() {
    const panelState = state.panels.venues;
    const items = panelState.items;
    const pageSize = panelState.pageSize;
    const page = panelState.currentPage;

    venuesListEl.innerHTML = "";
    venuesEmptyEl.hidden = true;

    if (!state.query) {
      venuesEmptyEl.textContent = "Run a search to see recommended venues.";
      venuesEmptyEl.hidden = false;
      return;
    }

    if (!items.length && !panelState.loading) {
      venuesEmptyEl.textContent = "No venues found for this query yet.";
      venuesEmptyEl.hidden = false;
      return;
    }

    const startIdx = (page - 1) * pageSize;
    const endIdx = startIdx + pageSize;
    const pageItems = items.slice(startIdx, endIdx);

    for (const v of pageItems) {
      const li = document.createElement("li");
      li.className = "venue-row";

      const top = document.createElement("div");
      top.className = "venue-main";

      const nameEl = document.createElement("div");
      nameEl.className = "venue-name";
      nameEl.textContent = v.name || v.source_id || "Unknown venue";

      const scoreEl = document.createElement("span");
      scoreEl.className = "pill";
      const totalScore =
        typeof v.total_score === "number" ? v.total_score : 0;
      scoreEl.textContent = `Total Score: ${totalScore.toFixed(2)}`;

      top.appendChild(nameEl);
      top.appendChild(scoreEl);
      li.appendChild(top);

      if (v.host_organization_name) {
        const hostEl = document.createElement("div");
        hostEl.className = "venue-host";
        hostEl.textContent = v.host_organization_name;
        li.appendChild(hostEl);
      }

      const bottom = document.createElement("div");
      bottom.className = "venue-bottom";

      const count = Array.isArray(v.papers) ? v.papers.length : 0;
      const countEl = document.createElement("span");
      countEl.className = "muted small";
      countEl.textContent = `${count} paper${count === 1 ? "" : "s"}`;

      if (v.openalex_url) {
        const link = document.createElement("a");
        link.className = "text-link";
        link.href = v.openalex_url;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.innerHTML =
          '<span class="icon">🌐</span> View on OpenAlex';
        bottom.appendChild(link);
      }

      bottom.appendChild(countEl);
      li.appendChild(bottom);

      venuesListEl.appendChild(li);
    }

    updatePaginationControls("venues");
  }

  async function loadMoreVenuesIfNeeded(showLoader = false) {
    const panelState = state.panels.venues;
    if (!panelState.hasMore || panelState.loading) return;

    panelState.loading = true;
    if (showLoader) venuesLoaderEl.hidden = false;

    try {
      const res = await fetchJSON("/api/search/venues", {
        query: state.query,
        limit: panelState.backendChunkSize,
        offset: panelState.backendOffset,
        paper_weight: state.paperWeight,
        concept_weight: state.conceptWeight,
      });

      const newItems = res.venues || [];
      const totalFromApi = res.total_sources;
      if (typeof totalFromApi === "number") {
        panelState.total = totalFromApi;
      }

      if (newItems.length < panelState.backendChunkSize) {
        panelState.hasMore = false;
      }

      panelState.items = panelState.items.concat(newItems);
      panelState.backendOffset += newItems.length;
    } catch (err) {
      console.error("Error fetching more venues:", err);
      panelState.hasMore = false;
    } finally {
      panelState.loading = false;
      if (showLoader) venuesLoaderEl.hidden = true;
    }
  }

  async function loadMoreAuthorsIfNeeded(showLoader = false) {
    const panelState = state.panels.authors;
    if (!panelState.hasMore || panelState.loading) return;

    panelState.loading = true;
    if (showLoader) authorsLoaderEl.hidden = false;

    try {
      const res = await fetchJSON("/api/search/authors", {
        query: state.query,
        limit: panelState.backendChunkSize,
        offset: panelState.backendOffset,
        paper_weight: state.paperWeight,
        concept_weight: state.conceptWeight,
      });

      const newItems = res.authors || [];
      const totalFromApi = res.total_authors;
      if (typeof totalFromApi === "number") {
        panelState.total = totalFromApi;
      }

      if (newItems.length < panelState.backendChunkSize) {
        panelState.hasMore = false;
      }

      panelState.items = panelState.items.concat(newItems);
      panelState.backendOffset += newItems.length;
    } catch (err) {
      console.error("Error fetching more authors:", err);
      panelState.hasMore = false;
    } finally {
      panelState.loading = false;
      if (showLoader) authorsLoaderEl.hidden = true;
    }
  }

  // ---------------------- Pagination controls ---------------------- //

  function updatePaginationControls(panelName) {
    const panelState = state.panels[panelName];
    const controls = document.querySelector(
      `.pagination-controls[data-panel="${panelName}"]`
    );
    if (!controls) return;

    const prevBtn = controls.querySelector(".pager-prev");
    const nextBtn = controls.querySelector(".pager-next");
    const label = controls.querySelector(".pager-label");

    const pageSize = panelState.pageSize;
    const page = panelState.currentPage;
    const itemsLoaded = panelState.items.length;
    const totalItems =
      panelState.total !== null ? panelState.total : itemsLoaded;

    const maxPage = Math.max(1, Math.ceil(totalItems / pageSize));

    if (label) {
      label.textContent = `Page ${page}`;
    }

    if (prevBtn) prevBtn.disabled = page <= 1;

    if (nextBtn) {
      const startIdxNext = page * pageSize;
      const moreInCache = startIdxNext < itemsLoaded;
      const canFetchMore = !!panelState.hasMore;
      nextBtn.disabled = !moreInCache && !canFetchMore;
    }
  }

  async function changePage(panelName, direction) {
    const panelState = state.panels[panelName];
    const pageSize = panelState.pageSize;
    const itemsLoaded = panelState.items.length;
    const totalItems =
      panelState.total !== null ? panelState.total : itemsLoaded;

    let newPage = panelState.currentPage + direction;
    if (newPage < 1) newPage = 1;

    // Going backwards => always cache-only
    if (direction < 0) {
      const maxPage = Math.max(1, Math.ceil(totalItems / pageSize));
      if (newPage > maxPage) newPage = maxPage;
      panelState.currentPage = newPage;
      if (panelName === "papers") {
        renderPapersPanel();
      } else if (panelName === "venues") {
        renderVenuesPanel();
      } else if (panelName === "authors") {
        renderAuthorsPanel();
      }
      maybePrefetch(panelName); // optional prefetch even when going back
      return;
    }

    // Going forwards
    const startIdxNextPage = (newPage - 1) * pageSize;

    if (startIdxNextPage < itemsLoaded) {
      // Already in cache
      panelState.currentPage = newPage;
      if (panelName === "papers") {
        renderPapersPanel();
      } else if (panelName === "venues") {
        renderVenuesPanel();
      } else if (panelName === "authors") {
        renderAuthorsPanel();
      }
      maybePrefetch(panelName);
      return;
    }

    // Need more from backend
    if (panelName === "papers") {
      await loadMorePapersIfNeeded(false); // background, no spinner
      const newItemsLoaded = state.panels.papers.items.length;
      if (startIdxNextPage < newItemsLoaded) {
        panelState.currentPage = newPage;
      }
      renderPapersPanel();
      maybePrefetch("papers");
    } else if (panelName === "venues") {
      await loadMoreVenuesIfNeeded(false);
      const newItemsLoaded = state.panels.venues.items.length;
      if (startIdxNextPage < newItemsLoaded) {
        panelState.currentPage = newPage;
      }
      renderVenuesPanel();
      maybePrefetch("venues");
    } else if (panelName === "authors") {
      await loadMoreAuthorsIfNeeded(false);
      const newItemsLoaded = state.panels.authors.items.length;
      if (startIdxNextPage < newItemsLoaded) {
        panelState.currentPage = newPage;
      }
      renderAuthorsPanel();
      maybePrefetch("authors");
    }
  }

  // prefetch “one page ahead” when possible
  function maybePrefetch(panelName) {
    if (panelName !== "papers" && panelName !== "venues" && panelName !== "authors") return;

    const panelState = state.panels[panelName];
    if (!panelState.hasMore || panelState.loading) return;

    const pageSize = panelState.pageSize;
    const nextPage = panelState.currentPage + 1;
    const nextStartIdx = (nextPage - 1) * pageSize;

    if (nextStartIdx >= panelState.items.length) {
      if (panelName === "papers") {
        loadMorePapersIfNeeded(false); // fire-and-forget, no loader
      } else if (panelName === "venues") {
        loadMoreVenuesIfNeeded(false);
      } else if (panelName === "authors") {
        loadMoreAuthorsIfNeeded(false);
      }
    }
  }

  paginationControls.forEach((controls) => {
    const panelName = controls.dataset.panel;
    const prevBtn = controls.querySelector(".pager-prev");
    const nextBtn = controls.querySelector(".pager-next");

    if (prevBtn) {
      prevBtn.addEventListener("click", () => changePage(panelName, -1));
    }
    if (nextBtn) {
      nextBtn.addEventListener("click", () => changePage(panelName, 1));
    }
  });

  // ---------------------- Page-size buttons ---------------------- //

  pageSizeGroups.forEach((group) => {
    const panelName = group.dataset.panel;
    const buttons = group.querySelectorAll(".page-size-btn");

    buttons.forEach((btn) => {
      btn.addEventListener("click", () => {
        buttons.forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");

        const size = parseInt(btn.dataset.size || "5", 10);
        state.panels[panelName].pageSize = size;
        state.panels[panelName].currentPage = 1;

        if (panelName === "papers") {
          renderPapersPanel();
          maybePrefetch("papers");
        } else if (panelName === "venues") {
          renderVenuesPanel();
          maybePrefetch("venues");
        } else if (panelName === "authors") {
          renderAuthorsPanel();
        }
      });
    });
  });

  // ---------------------- Search submit ---------------------- //

  function triggerSearch(){
    const q = searchInput ? searchInput.value.trim() : "";
      if (!q) return;

      const searchType = searchTypeSelect ? searchTypeSelect.value : "hybrid";
      const paperWeight = parseFloat(paperWeightInput?.value || "0.8");
      const conceptWeight = parseFloat(conceptWeightInput?.value || "0.2");

      state.query = q;
      state.searchType = searchType;
      state.paperWeight = paperWeight;
      state.conceptWeight = conceptWeight;

      // reset panel states
      state.panels.papers.items = [];
      state.panels.papers.total = null;
      state.panels.papers.backendOffset = 0;
      state.panels.papers.currentPage = 1;
      state.panels.papers.hasMore = true;

      state.panels.venues.items = [];
      state.panels.venues.total = null;
      state.panels.venues.backendOffset = 0;
      state.panels.venues.currentPage = 1;
      state.panels.venues.hasMore = true;

      state.panels.authors.items = [];
      state.panels.authors.total = null;
      state.panels.authors.currentPage = 1;

      // loaders ONLY here (initial search)
      papersLoaderEl.hidden = false;
      venuesLoaderEl.hidden = false;
      authorsLoaderEl.hidden = false;
      papersEmptyEl.hidden = true;
      venuesEmptyEl.hidden = true;
      authorsEmptyEl.hidden = true;

      const payloadBase = {
        query: q,
        paper_weight: paperWeight,
        concept_weight: conceptWeight,
      };

      const papersPromise = (async () => {
        try {
          const res = await fetchJSON("/api/search/papers", {
            ...payloadBase,
            search_type: searchType,
            limit: state.panels.papers.backendChunkSize,
            offset: 0,
          });
          const items = res.papers || [];
          state.panels.papers.items = items;
          state.panels.papers.total =
            typeof res.total_papers === "number"
              ? res.total_papers
              : null;
          state.panels.papers.backendOffset = items.length;
          if (items.length < state.panels.papers.backendChunkSize) {
            state.panels.papers.hasMore = false;
          }
          renderPapersPanel();
          maybePrefetch("papers");
        } catch (err) {
          console.error("Error searching papers:", err);
          papersEmptyEl.textContent = "Error loading papers.";
          papersEmptyEl.hidden = false;
          state.panels.papers.hasMore = false;
        } finally {
          papersLoaderEl.hidden = true;
          setPanelLoading('papers', false);
        }
      })();

      const authorsPromise = (async () => {
        try {
          const res = await fetchJSON("/api/search/authors", {
            ...payloadBase,
          });
          const items = res.authors || [];
          state.panels.authors.items = items;
          state.panels.authors.total =
            typeof res.total_authors === "number"
              ? res.total_authors
              : null;
          renderAuthorsPanel();
        }
        catch (err) {
          console.error("Error searching authors:", err);
          authorsEmptyEl.textContent = "Error loading authors.";
          authorsEmptyEl.hidden = false;
        }
        finally {
          authorsLoaderEl.hidden = true;
      setPanelLoading('authors', false);
        }
      })();

      const venuesPromise = (async () => {
        try {
          const res = await fetchJSON("/api/search/venues", {
            ...payloadBase,
          });
          const items = res.venues || [];
          state.panels.venues.items = items;
          state.panels.venues.total =
            typeof res.total_sources === "number"
              ? res.total_sources
              : null;
          state.panels.venues.backendOffset = items.length;
          if (items.length < state.panels.venues.backendChunkSize) {
            state.panels.venues.hasMore = false;
          }
          renderVenuesPanel();
          maybePrefetch("venues");
        } catch (err) {
          console.error("Error searching venues:", err);
          venuesEmptyEl.textContent = "Error loading venues.";
          venuesEmptyEl.hidden = false;
          state.panels.venues.hasMore = false;
        } finally {
          venuesLoaderEl.hidden = true;
      setPanelLoading('venues', false);

        }
      })();

      Promise.allSettled([papersPromise, authorsPromise, venuesPromise]).then(() => {
        // all done
      });
  }
  if (searchForm){
    searchForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      
      if (papersLoaderEl) papersLoaderEl.hidden = false;
      if (venuesLoaderEl) venuesLoaderEl.hidden = false;
      if (authorsLoaderEl) authorsLoaderEl.hidden = true;
      if (papersEmptyEl) papersEmptyEl.hidden = true;
      if (venuesEmptyEl) venuesEmptyEl.hidden = true;
      if (authorsEmptyEl) authorsEmptyEl.hidden = true;
      if (papersListEl) papersListEl.innerHTML = "";
      if (venuesListEl) venuesListEl.innerHTML = "";
      if (authorsListEl) authorsListEl.innerHTML = "";

      setPanelLoading('papers', true);
      setPanelLoading('authors', true);
      setPanelLoading('venues', true);

      triggerSearch();
    });
  }
  if (searchBtn) {
    searchBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      
      if (papersLoaderEl) papersLoaderEl.hidden = false;
      if (venuesLoaderEl) venuesLoaderEl.hidden = false;
      if (authorsLoaderEl) authorsLoaderEl.hidden = true;
      if (papersEmptyEl) papersEmptyEl.hidden = true;
      if (venuesEmptyEl) venuesEmptyEl.hidden = true;
      if (authorsEmptyEl) authorsEmptyEl.hidden = true;
      if (papersListEl) papersListEl.innerHTML = "";
      if (venuesListEl) venuesListEl.innerHTML = "";
      if (authorsListEl) authorsListEl.innerHTML = "";

      setPanelLoading('papers', true);
      setPanelLoading('authors', true);
      setPanelLoading('venues', true);

      triggerSearch();
    });
  }
})();