/**
 * ObjectSearchWidget - anywidget implementation
 * Search and select geoscience objects by name with metadata display
 */

function render({ model, el }) {
    // Create main container
    const container = document.createElement("div");
    container.className = "evo-object-search-widget";

    // Row 1: Search input and type filter
    const searchRow = document.createElement("div");
    searchRow.className = "evo-object-search-row";

    const searchLabel = document.createElement("label");
    searchLabel.className = "evo-object-search-label";
    searchLabel.textContent = "Search:";

    const searchInput = document.createElement("input");
    searchInput.type = "text";
    searchInput.className = "evo-object-search-input";
    searchInput.placeholder = "Type to search objects by name...";
    searchInput.value = model.get("search_text") || "";

    const typeLabel = document.createElement("label");
    typeLabel.className = "evo-object-search-label";
    typeLabel.textContent = "Type:";

    const typeSelect = document.createElement("select");
    typeSelect.className = "evo-object-search-type-select";

    const loadingIndicator = document.createElement("span");
    loadingIndicator.className = "evo-object-search-loading";
    loadingIndicator.textContent = "";

    searchRow.appendChild(searchLabel);
    searchRow.appendChild(searchInput);
    searchRow.appendChild(typeLabel);
    searchRow.appendChild(typeSelect);
    searchRow.appendChild(loadingIndicator);

    // Row 2: Results dropdown and status
    const resultsRow = document.createElement("div");
    resultsRow.className = "evo-object-search-row";

    const resultsLabel = document.createElement("label");
    resultsLabel.className = "evo-object-search-label";
    resultsLabel.textContent = "Object:";

    const resultsSelect = document.createElement("select");
    resultsSelect.className = "evo-object-search-results-select";
    resultsSelect.disabled = true;

    const statusLabel = document.createElement("span");
    statusLabel.className = "evo-object-search-status";
    statusLabel.textContent = "";

    resultsRow.appendChild(resultsLabel);
    resultsRow.appendChild(resultsSelect);
    resultsRow.appendChild(statusLabel);

    // Metadata display area
    const metadataOutput = document.createElement("pre");
    metadataOutput.className = "evo-object-search-metadata";

    container.appendChild(searchRow);
    container.appendChild(resultsRow);
    container.appendChild(metadataOutput);
    el.appendChild(container);

    // Debounce timer
    let debounceTimer = null;

    // Update type options
    function updateTypeOptions() {
        const typeOptions = model.get("type_options") || [];
        const currentType = model.get("object_type");

        typeSelect.innerHTML = "";
        typeOptions.forEach(([text, value]) => {
            const option = document.createElement("option");
            option.textContent = text;
            option.value = value === null ? "" : value;
            if (value === currentType) {
                option.selected = true;
            }
            typeSelect.appendChild(option);
        });
    }

    // Update results options
    function updateResultsOptions() {
        const resultOptions = model.get("result_options") || [];
        const currentValue = model.get("selected_id");

        resultsSelect.innerHTML = "";
        resultOptions.forEach(([text, value]) => {
            const option = document.createElement("option");
            option.textContent = text;
            option.value = value === null ? "" : value;
            if (value === currentValue) {
                option.selected = true;
            }
            resultsSelect.appendChild(option);
        });

        resultsSelect.disabled = resultOptions.length === 0 || 
            (resultOptions.length === 1 && resultOptions[0][1] === null);
    }

    function updateLoading() {
        const isLoading = model.get("loading") || false;
        loadingIndicator.textContent = isLoading ? "Loading..." : "";
    }

    function updateStatus() {
        statusLabel.textContent = model.get("status_text") || "";
    }

    function updateMetadata() {
        metadataOutput.textContent = model.get("metadata_text") || "";
    }

    function updateSearchText() {
        searchInput.value = model.get("search_text") || "";
    }

    // Event handlers
    searchInput.addEventListener("input", () => {
        if (debounceTimer) {
            clearTimeout(debounceTimer);
        }
        debounceTimer = setTimeout(() => {
            model.set("search_text", searchInput.value);
            model.save_changes();
        }, 300);
    });

    typeSelect.addEventListener("change", () => {
        const value = typeSelect.value === "" ? null : typeSelect.value;
        model.set("object_type", value);
        model.save_changes();
    });

    resultsSelect.addEventListener("change", () => {
        const value = resultsSelect.value === "" ? null : resultsSelect.value;
        model.set("selected_id", value);
        model.save_changes();
    });

    // Initial updates
    updateTypeOptions();
    updateResultsOptions();
    updateLoading();
    updateStatus();
    updateMetadata();

    // Listen for changes
    model.on("change:type_options", updateTypeOptions);
    model.on("change:object_type", updateTypeOptions);
    model.on("change:result_options", updateResultsOptions);
    model.on("change:selected_id", updateResultsOptions);
    model.on("change:loading", updateLoading);
    model.on("change:status_text", updateStatus);
    model.on("change:metadata_text", updateMetadata);
    model.on("change:search_text", updateSearchText);

    return () => {
        if (debounceTimer) {
            clearTimeout(debounceTimer);
        }
        model.off("change:type_options", updateTypeOptions);
        model.off("change:object_type", updateTypeOptions);
        model.off("change:result_options", updateResultsOptions);
        model.off("change:selected_id", updateResultsOptions);
        model.off("change:loading", updateLoading);
        model.off("change:status_text", updateStatus);
        model.off("change:metadata_text", updateMetadata);
        model.off("change:search_text", updateSearchText);
    };
}

export default { render };
