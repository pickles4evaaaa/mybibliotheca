// Library page integration: instant render from IndexedDB + background refresh
(function(){
  if(!window.CatalogCache){ return; }
  const worker = new Worker('/static/catalog-worker.js');
  const userId = (document.querySelector('meta[name="user-id"]').getAttribute('content')) || 'me';

  const clientGrid = document.getElementById('client-books-container');
  if(!clientGrid){ return; }

  let state = { items: [], order: [], page: 1, perPage: parseInt(clientGrid.dataset.perPage||'60',10), selected: new Set() };
  // Virtualization settings
  const CARD_GAP = 16; // approx Bootstrap g-4 gap
  let cardHeight = 320; // default estimate; will measure after first render
  let cols = 0;
  let total = 0;
  let lastQuery = { search: '', status: 'all', sort: 'title_asc' };
  let activated = false; // switch to client grid only after first non-empty render
  const initialPage = (()=>{ try { return parseInt(new URLSearchParams(window.location.search).get('page')||'1', 10);} catch(_){ return 1; }})();
  let didInitialPageScroll = false;

  // Selection toolbar elements (shared with SSR)
  const selectionToolbar = document.getElementById('selection-toolbar');
  const selectionCount = document.getElementById('selection-count');

  // Basic virtualization: container gets a tall spacer; we render only visible rows into an inner layer
  const spacer = document.createElement('div');
  const inner = document.createElement('div');
  inner.className = 'row g-4';
  clientGrid.appendChild(spacer);
  clientGrid.appendChild(inner);
  clientGrid.style.minHeight = '200px';

  function computeCols(){
    // approximate based on current breakpoints by measuring first rendered card or container width
    const width = clientGrid.clientWidth || clientGrid.getBoundingClientRect().width || 1;
    // Bootstrap col-6 on xs -> 2, col-sm-4 -> 3, col-md-3 -> 4, col-lg-2 -> 6 (max)
    if(width >= 1200) return 6; // lg
    if(width >= 992) return 4;  // md
    if(width >= 576) return 3;  // sm
    return 2;                   // xs
  }

  function renderCard(book){
    const col = document.createElement('div');
    col.className = 'col-lg-2 col-md-3 col-sm-4 col-6 book-item';
  // Canonicalize reading status; keep empty as default (no personal status)
    const canon = (s)=>{
      try{ return (s||'').toString().trim().toLowerCase().replace(/[-\s]+/g,'_'); }catch(_){ return null; }
    };
  let rs = canon(book.reading_status);
  if(!rs || rs === 'unknown' || rs === 'library_only') rs = '';
    // Map common synonyms to canonical values
    if(rs === 'want_to_read' || rs === 'wishlist_reading') rs = 'plan_to_read';
    if(rs === 'currently reading') rs = 'currently_reading';
    if(rs === 'on-hold' || rs === 'paused') rs = 'on_hold';
    if(rs === 'finished' || rs === 'complete' || rs === 'completed') rs = 'read';
  col.dataset.readingStatus = rs;
    col.dataset.ownershipStatus = book.ownership_status || 'owned';
    const checked = state.selected.has(String(book.id)) ? 'checked' : '';
    col.innerHTML = `
      <div class="card h-100 book-card shadow-sm position-relative" data-book-id="${book.id}">
        <div class="position-absolute top-0 end-0 p-2" style="z-index:10; pointer-events: none;">
          <input type="checkbox" class="form-check-input book-checkbox" value="${book.id}" ${checked} style="pointer-events:auto;">
        </div>
        <div class="book-cover-wrapper position-relative" style="cursor:pointer;">
          <img alt="cover" class="card-img-top book-cover" loading="lazy" src="${book.cover_url || '/static/bookshelf.png'}" />
        </div>
        <div class="card-body p-2" style="cursor:pointer;">
          <h6 class="card-title mb-1 book-title">${book.title || ''}</h6>
          ${book.author ? `<div class=\"mb-1 small text-muted\">${book.author}</div>` : ''}
        </div>
        <a href="/view_book_enhanced/${book.id}" class="stretched-link" aria-label="View book"></a>
      </div>
    `;
    return col;
  }

  function ensureMeasurements(){
    if(cardHeight && cols) return;
    // Render one sample to measure
    inner.innerHTML = '';
    const sampleIdx = state.order.length ? state.order[0] : 0;
    const sample = state.items[sampleIdx];
    if(sample){
      const el = renderCard(sample);
      inner.appendChild(el);
      const rect = el.getBoundingClientRect();
      if(rect.height) cardHeight = Math.ceil(rect.height) + CARD_GAP; // include gap
      inner.innerHTML = '';
    }
    cols = computeCols();
  }

  function renderVirtual(){
    ensureMeasurements();
    if(!cols) cols = computeCols();
  const rowCount = Math.ceil(total / Math.max(1, cols));
  // When the window scrolls (most likely), compute scroll relative to the grid's top
  const gridRect = clientGrid.getBoundingClientRect();
  const gridTopAbs = gridRect.top + window.scrollY;
  const viewportH = window.innerHeight;
  const scrollTop = Math.max(0, (clientGrid.scrollTop || (window.scrollY - gridTopAbs)));
    // Compute visible row window
    const startRow = Math.max(0, Math.floor(scrollTop / Math.max(1, cardHeight)) - 2);
    const endRow = Math.min(rowCount, startRow + Math.ceil(viewportH / Math.max(1, cardHeight)) + 4);
    const startIdx = startRow * cols;
    const endIdx = Math.min(total, endRow * cols);

    // Position inner layer via top padding placeholder
    spacer.style.height = (startRow * cardHeight) + 'px';
    inner.style.transform = 'translateY(0)';
    inner.innerHTML = '';
    const frag = document.createDocumentFragment();
    for(let i=startIdx;i<endIdx;i++){
      const itemIdx = state.order[i];
      if(itemIdx == null) break;
      const book = state.items[itemIdx];
      frag.appendChild(renderCard(book));
    }
    inner.appendChild(frag);
    const renderedCount = Math.max(0, endIdx - startIdx);
    const afterRows = Math.max(0, rowCount - endRow);
    // Add an after spacer by padding-bottom to clientGrid
    clientGrid.style.paddingBottom = (afterRows * cardHeight) + 'px';

    // On first render after data/measurements, if a server page is provided, scroll to that segment
    if(!didInitialPageScroll && initialPage > 1){
      const startIndex = Math.max(0, (initialPage - 1) * Math.max(1, state.perPage));
      const targetRow = Math.floor(startIndex / Math.max(1, cols));
      const desiredTop = gridTopAbs + (targetRow * Math.max(1, cardHeight));
      // Prevent recursive loops: set flag before scrolling
      didInitialPageScroll = true;
      window.scrollTo({ top: desiredTop, behavior: 'auto' });
      // Re-render after scroll to align the window slice
      setTimeout(renderVirtual, 0);
      return;
    }

    // First activation: only swap to client grid after we actually rendered items
    if(!activated && renderedCount > 0){
      activated = true;
      clientGrid.style.display = '';
      const serverGrid = document.getElementById('books-container');
      if(serverGrid){ serverGrid.style.display = 'none'; }
    }

  // Sync selection toolbar after re-render
  updateSelectionToolbar();
  }

  function runSearch(){
    const urlParams = new URLSearchParams(window.location.search);
    const status = urlParams.get('status_filter') || 'all';
    const sort = urlParams.get('sort') || 'title_asc';
    const search = urlParams.get('search') || '';
    lastQuery = { status, sort, search };
    // Request only ordering for virtualization
    worker.postMessage({ type: 'search', mode: 'order', items: state.items, query: search, filters: { status }, sort });
  }

  // Public helper: update URL params and trigger instant client filtering
  function updateURLAndSearch(partial){
    const url = new URL(window.location.href);
    if(partial && typeof partial === 'object'){
      if(Object.prototype.hasOwnProperty.call(partial, 'status')){
        url.searchParams.set('status_filter', partial.status || 'all');
      }
      if(Object.prototype.hasOwnProperty.call(partial, 'sort')){
        url.searchParams.set('sort', partial.sort || 'title_asc');
      }
      if(Object.prototype.hasOwnProperty.call(partial, 'search')){
        const val = partial.search || '';
        if(val){ url.searchParams.set('search', val); }
        else { url.searchParams.delete('search'); }
      }
      history.replaceState(null, '', url.toString());
    }
    runSearch();
  }

  worker.onmessage = (e)=>{
    const msg = e.data||{};
    if(msg.type === 'results' || msg.type === 'order'){
      total = msg.total || 0;
      if(msg.type === 'results'){
        // Back-compat path (not currently used)
        state.order = Array.from({length: msg.items.length}, (_,i)=>i);
      } else {
        state.order = msg.order || [];
      }
  // For plan_to_read, render asap to reduce perceived lag
  renderVirtual();
      const pct = document.getElementById('client-total');
      if(pct){ pct.textContent = `${total}`; }
    }
  };

  // Initial load
  CatalogCache.load(userId).then(payload => {
    state.items = payload.items||[];
    runSearch();
    // Background refresh
    CatalogCache.refresh(userId).then(p => {
      state.items = p.items||[];
      runSearch();
    }).catch(()=>{});
  }).catch(()=>{});

  // If admin cleared server-side cache in another tab, refresh our local cache too
  window.addEventListener('storage', (e)=>{
    if(e && e.key === 'mb_catalog_server_cache_cleared'){
      // Force a full refresh path: clear local cache then reload fresh index
      if(window.CatalogCache && typeof window.CatalogCache.clear === 'function'){
        window.CatalogCache.clear().then(()=>{
          CatalogCache.load(userId).then(payload => {
            state.items = payload.items || [];
            runSearch();
            // Kick one more refresh to pick up any deltas
            CatalogCache.refresh(userId).then(p => { state.items = p.items||[]; runSearch(); }).catch(()=>{});
          }).catch(()=>{});
        }).catch(()=>{});
      }
    }
  });

  // Optional: local cache reset helper bound for settings or debug
  window.resetLocalLibraryCache = async function(){
    if(!(window.CatalogCache && typeof window.CatalogCache.clear === 'function')) return;
    const ok = await window.CatalogCache.clear();
    if(ok){ window.location.reload(); }
  };

  // Expose minimal API for template to trigger instant client filtering
  window.LibraryClient = { updateURLAndSearch };

  // Re-render on resize/scroll for virtualization
  let resizeTO, scrollTO;
  window.addEventListener('resize', ()=>{
    clearTimeout(resizeTO);
    resizeTO = setTimeout(()=>{
      cols = computeCols();
      renderVirtual();
    }, 150);
  });
  // If the main window scrolls, update; if a specific scroll container is used, also listen to it
  window.addEventListener('scroll', ()=>{
    clearTimeout(scrollTO);
    scrollTO = setTimeout(renderVirtual, 50);
  }, { passive: true });
  clientGrid.addEventListener('scroll', ()=>{
    clearTimeout(scrollTO);
    scrollTO = setTimeout(renderVirtual, 50);
  }, { passive: true });

  // Delegated selection handling on client grid
  clientGrid.addEventListener('change', (e)=>{
    const target = e.target;
    if(!(target && target.classList && target.classList.contains('book-checkbox'))) return;
    const id = String(target.value);
    if(target.checked){ state.selected.add(id); }
    else { state.selected.delete(id); }
    updateSelectionToolbar();
  });

  function getVisibleClientCheckboxes(){
    return inner.querySelectorAll('.book-checkbox');
  }

  function updateSelectionToolbar(){
    if(!selectionToolbar || !selectionCount) return;
    const count = state.selected.size;
    selectionCount.textContent = String(count);
    selectionToolbar.style.display = count > 0 ? 'block' : 'none';
  }

  // Override global selection helpers to support client grid
  window.selectAll = function(){
    // Prefer client grid when active
    const serverGrid = document.getElementById('books-container');
    const usingClient = clientGrid && clientGrid.style.display !== 'none' && (!serverGrid || serverGrid.style.display === 'none');
    if(usingClient){
      const boxes = getVisibleClientCheckboxes();
      boxes.forEach(cb => { cb.checked = true; state.selected.add(String(cb.value)); });
      updateSelectionToolbar();
    } else {
      // Fallback to SSR behavior
      const serverContainer = document.getElementById('books-container');
      if(serverContainer){
        serverContainer.querySelectorAll('.book-item:not([style*="display: none"]) .book-checkbox').forEach(cb => cb.checked = true);
      }
      // Let existing SSR code update toolbar if present
      updateSelectionToolbar();
    }
  };

  window.clearSelection = function(){
    const serverGrid = document.getElementById('books-container');
    const usingClient = clientGrid && clientGrid.style.display !== 'none' && (!serverGrid || serverGrid.style.display === 'none');
    if(usingClient){
      state.selected.clear();
      getVisibleClientCheckboxes().forEach(cb => { cb.checked = false; });
      updateSelectionToolbar();
    } else {
      const checkboxes = serverGrid ? serverGrid.querySelectorAll('.book-checkbox') : [];
      checkboxes.forEach(cb => cb.checked = false);
      updateSelectionToolbar();
    }
  };

  window.deleteSelected = function(){
    // Use selected IDs from current mode
    const ids = Array.from(state.selected);
    if(ids.length === 0){
      // Fallback: try SSR selection if any
      const serverGrid = document.getElementById('books-container');
      const ssrIds = serverGrid ? Array.from(serverGrid.querySelectorAll('.book-checkbox:checked')).map(cb=>cb.value) : [];
      if(ssrIds.length === 0) return;
      submitBulkDelete(ssrIds);
      return;
    }
    submitBulkDelete(ids);
  };

  function submitBulkDelete(ids){
    if(!confirm(`Are you sure you want to delete ${ids.length} book(s) from your library?`)) return;
    const form = document.getElementById('bulk-delete-form');
    if(!form) return;
    // Clear existing inputs
    form.querySelectorAll('input[name="book_ids"]').forEach(input => input.remove());
    // Add selected book IDs
    ids.forEach(id => {
      const input = document.createElement('input');
      input.type = 'hidden';
      input.name = 'book_ids';
      input.value = id;
      form.appendChild(input);
    });
    form.submit();
  }
})();
