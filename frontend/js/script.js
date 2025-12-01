let lat = 26.4499, lon = 80.3319;
window.onload = () => {
    navigator.geolocation.getCurrentPosition(p => {
        lat = p.coords.latitude; lon = p.coords.longitude;
        document.getElementById('status').innerText = "✅ GPS Ready";
    });
};
async function search() {
    const q = document.getElementById('query').value;
    document.getElementById('loader').classList.remove('hidden');
    document.getElementById('results').innerHTML = '';
    
    try {
        const res = await fetch('http://127.0.0.1:5000/api/recommend', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: q, latitude: lat, longitude: lon})
        });
        const data = await res.json();
        document.getElementById('loader').classList.add('hidden');
        
        let html = `<h2>Diagnosis: <span style="color:#38bdf8">${data.disease_detected}</span></h2>`;
        data.hospitals.forEach(h => {
            html += `<div class="card">
                <h3>${h.name} <span style="font-size:0.8em; color:#34d399">${h.is_nabh_accredited?'(NABH)':''}</span></h3>
                <p>${h.address}</p>
                <div style="margin-top:10px; display:flex; gap:15px; color:#cbd5e1">
                    <span>🚗 ${h.time_car} min</span> <span>📍 ${h.distance_km} km</span>
                    <a href="http://maps.google.com/?q=${h.latitude},${h.longitude}" target="_blank" style="color:#38bdf8">Navigate</a>
                    <span class="cost">₹${h.estimated_cost}</span>
                </div>
            </div>`;
        });
        document.getElementById('results').innerHTML = html;
    } catch(e) { alert("Server Error"); document.getElementById('loader').classList.add('hidden'); }
}