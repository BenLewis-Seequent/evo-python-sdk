/**
 * FeedbackWidget - anywidget implementation
 * Displays progress bar with label and message
 */

function render({ model, el }) {
    // Create container
    const container = document.createElement("div");
    container.className = "evo-feedback-widget";

    // Label
    const label = document.createElement("span");
    label.className = "evo-feedback-label";
    label.textContent = model.get("label") || "";

    // Progress container
    const progressContainer = document.createElement("div");
    progressContainer.className = "evo-feedback-progress-container";

    // Progress bar
    const progressBar = document.createElement("div");
    progressBar.className = "evo-feedback-progress-bar";
    progressBar.style.width = "0%";

    // Progress text
    const progressText = document.createElement("span");
    progressText.className = "evo-feedback-progress-text";
    progressText.textContent = "0.0%";

    progressContainer.appendChild(progressBar);
    progressContainer.appendChild(progressText);

    // Message
    const message = document.createElement("span");
    message.className = "evo-feedback-message";
    message.textContent = model.get("message") || "";

    container.appendChild(label);
    container.appendChild(progressContainer);
    container.appendChild(message);
    el.appendChild(container);

    // Update function
    function updateProgress() {
        const progress = model.get("progress") || 0;
        const percentage = Math.min(Math.max(progress * 100, 0), 100);
        progressBar.style.width = `${percentage}%`;
        progressText.textContent = `${percentage.toFixed(1)}%`;
    }

    function updateMessage() {
        message.textContent = model.get("message") || "";
    }

    function updateLabel() {
        label.textContent = model.get("label") || "";
    }

    // Initial update
    updateProgress();
    updateMessage();
    updateLabel();

    // Listen for changes
    model.on("change:progress", updateProgress);
    model.on("change:message", updateMessage);
    model.on("change:label", updateLabel);

    return () => {
        model.off("change:progress", updateProgress);
        model.off("change:message", updateMessage);
        model.off("change:label", updateLabel);
    };
}

export default { render };
