let lat = 26.4499, lon = 80.3319; // Default Kanpur

window.onload = () => {
    // 1. Check Backend Health
    fetch('http://127.0.0.1:5000/')
        .catch(() => {
            alert("Error: Backend is offline! Run 'python app.py'");
        });

    // 2. Get GPS Location
    if(navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (p) => {
                lat = p.coords.latitude; 
                lon = p.coords.longitude;
                
                // Update UI to show Success
                const status = document.getElementById('gps-status');
                status.innerHTML = `<i class="fas fa-wifi"></i> GPS Signal Active`;
                status.className = "status-badge connected";
                
                // Show Coordinates
                document.getElementById('user-coords').classList.remove('hidden');
                document.getElementById('lat-val').innerText = lat.toFixed(4);
                document.getElementById('lon-val').innerText = lon.toFixed(4);
            }, 
            () => {
                // Handle Error
                const status = document.getElementById('gps-status');
                status.innerHTML = `<i class="fas fa-ban"></i> Location Access Denied`;
                status.className = "status-badge error";
            }
        );
    }
};

async function search() {
    const q = document.getElementById('query').value;
    if(!q) return alert("Please enter symptoms");
    
    document.getElementById('loader').classList.remove('hidden');
    document.getElementById('results').innerHTML = '';
    
    try {
        const res = await fetch('http://127.0.0.1:5000/api/recommend', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: q, latitude: lat, longitude: lon})
        });
        
        const data = await res.json();
        document.getElementById('loader').classList.add('hidden');
        
        if(!res.ok) throw new Error(data.error || "Server Error");
        
        let html = `<h2 style="margin-bottom:20px; color:#fff">Diagnosis: <span style="color:#38bdf8">${data.disease_detected}</span></h2>`;
        
        if(data.hospitals.length === 0) html += "<p>No hospitals found nearby.</p>";

        data.hospitals.forEach(h => {
            html += `
            <div class="card">
                <div class="card-header">
                    <div>
                        <h3>${h.name}</h3>
                        ${h.is_nabh_accredited ? '<span class="badge">NABH Certified</span>' : ''}
                    </div>
                    <div class="score">${h.total_score}/10</div>
                </div>
                <div class="address"><i class="fas fa-map-pin"></i> ${h.address}</div>
                
                <div class="stats-row">
                    <div class="stat"><i class="fas fa-car"></i> ${h.time_car} min</div>
                    <div class="stat"><i class="fas fa-motorcycle"></i> ${h.time_bike} min</div>
                    <div class="stat highlight">${h.distance_km} km</div>
                </div>

                <div class="action-row">
                    <a href="https://www.google.com/maps/dir/?api=1&destination=${h.latitude},${h.longitude}" target="_blank" class="nav-btn">
                        <i class="fas fa-location-arrow"></i> Navigate
                    </a>
                    <div class="cost">₹${h.estimated_cost.toLocaleString()}</div>
                </div>
            </div>`;
        });
        document.getElementById('results').innerHTML = html;

    } catch(e) { 
        console.error(e);
        document.getElementById('loader').classList.add('hidden');
        alert("Error: " + e.message); 
    }
}