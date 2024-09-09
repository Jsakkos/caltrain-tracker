// Function to reveal sections on scroll
function revealSections() {
    const sections = document.querySelectorAll('.section');
    const options = {
        threshold: 0.1 // Trigger when 10% of the section is in view
    };

    const observer = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('active');
                observer.unobserve(entry.target); // Stop observing after revealing
            }
        });
    }, options);

    sections.forEach(section => {
        observer.observe(section);
    });
}

// Function to load iframe content when the section is visible
function loadIframeContent() {
    const iframes = document.querySelectorAll('iframe');

    const observer = new IntersectionObserver((entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const iframe = entry.target;
                const src = iframe.getAttribute('data-src');
                iframe.setAttribute('src', src); // Load the iframe content
                observer.unobserve(iframe); // Stop observing after loading
            }
        });
    });

    iframes.forEach(iframe => {
        observer.observe(iframe);
    });
}

// Load on-time performance percentage
fetch('static/on_time_percentage.txt')
    .then(response => response.text())
    .then(data => {
        document.getElementById('on-time-performance-percentage').innerText = `${data}`;
    });
// Load start date
fetch('static/start_date.txt')
    .then(response => response.text())
    .then(data => {
        document.getElementById('start_date').innerText = `${data}`;
    });
// Load stop date
fetch('static/stop_date.txt')
    .then(response => response.text())
    .then(data => {
        document.getElementById('stop_date').innerText = `${data}`;
    });
// Load number of datapoints
fetch('static/n_datapoints.txt')
    .then(response => response.text())
    .then(data => {
        document.getElementById('n_datapoints').innerText = `${data}`;
    });

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    revealSections();
    loadIframeContent();
});
