// Initialize the map
const map = L.map('map').setView([37.7749, -122.4194], 10); // Centered around San Francisco

// Add a tile layer to the map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// Your 511.org API token
const apiToken = 'afec7635-a79b-4ccb-b87b-5c8d9cf5b36c';
// Fetch GTFS-rt data from 511.org through CORS Anywhere proxy
async function fetchGTFSRTData() {
    const response = await fetch(`http://localhost:8080/https://api.511.org/transit/vehiclepositions?api_key=${apiToken}&agency=Caltrain`, {
        headers: {
            'Authorization': `Bearer ${apiToken}`
        }
    });
    const arrayBuffer = await response.arrayBuffer();
    const root = await protobuf.load('https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto');
    const FeedMessage = root.lookupType('transit_realtime.FeedMessage');
    const message = FeedMessage.decode(new Uint8Array(arrayBuffer));
    return FeedMessage.toObject(message, { longs: String, enums: String, bytes: String });
}

// Parse GTFS-rt data to extract train locations
function parseGTFSRTData(data) {
    const trains = [];
    data.entity.forEach(entity => {
        if (entity.vehicle) {
            trains.push({
                lat: entity.vehicle.position.latitude,
                lon: entity.vehicle.position.longitude,
                id: entity.vehicle.vehicle.id,
                status: entity.vehicle.currentStatus
            });
        }
    });
    return trains;
}

// Function to update the map with train locations
async function updateMap() {
    const gtfsRTData = await fetchGTFSRTData();
    const trains = parseGTFSRTData(gtfsRTData);

    // Clear existing train markers
    map.eachLayer((layer) => {
        if (layer.options && layer.options.icon && layer.options.icon.options.className === 'train-icon') {
            map.removeLayer(layer);
        }
    });

    // Add train markers to the map
    trains.forEach(train => {
        const marker = L.marker([train.lat, train.lon], {
            icon: L.icon({
                iconUrl: 'train-icon.png', // URL to a train icon
                iconSize: [25, 25], // Adjust icon size
                iconAnchor: [12, 12],
                tooltipAnchor: [0, -12],
                className: 'train-icon'
            })
        }).addTo(map);
        marker.bindTooltip(`${train.id}: ${train.status}`);
    });
}

// Initial load of map data
updateMap();

// Optionally, set an interval to update train locations every minute
setInterval(updateMap, 60000);
