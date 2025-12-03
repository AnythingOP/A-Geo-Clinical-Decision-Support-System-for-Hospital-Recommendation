from math import radians, sin, cos, sqrt, atan2
import requests
import random

class RankingService:
    def __init__(self, db):
        self.db = db

    def _haversine(self, lat1, lon1, lat2, lon2):
        # Accurate Math Calculation
        if not lat1 or not lon1 or not lat2 or not lon2: return 999.0
        R = 6371.0 # Earth radius
        dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c

    def _get_route_data(self, lat1, lon1, lat2, lon2):
        """
        Tries OSRM API. If it fails/times out, falls back to Math.
        """
        try:
            # 1. Try OSRM API (Free)
            url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
            resp = requests.get(url, timeout=1.5) # Short timeout to keep app fast
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get('routes'):
                    dist = data['routes'][0]['distance'] / 1000 # Meters to Km
                    time = data['routes'][0]['duration'] / 60   # Seconds to Min
                    return round(dist, 1), int(time)
        except:
            pass
        
        # 2. Fallback: Smart Math Estimation
        # Real roads are ~30% longer than straight lines
        dist = self._haversine(lat1, lon1, lat2, lon2) * 1.3
        
        # Avg speed in city traffic = 25 km/h
        time = int((dist / 25) * 60) + 5 # +5 mins for parking/traffic lights
        
        return round(dist, 1), time

    def rank_hospitals(self, t_id, u_lat, u_lon):
        hosps = self.db.get_hospitals_by_treatment(t_id)
        if not hosps: return []

        # 1. Pre-filter using Math (Fast)
        # Only check hospitals within 50km straight line
        candidates = []
        for h in hosps:
            h['straight_dist'] = self._haversine(u_lat, u_lon, h['latitude'], h['longitude'])
            if h['straight_dist'] < 50: 
                candidates.append(h)
        
        # Sort by proximity and take top 20 for API check
        top_candidates = sorted(candidates, key=lambda x: x['straight_dist'])[:20]

        processed = []
        for h in top_candidates:
            # 2. Get Accurate Road Data
            dist, time = self._get_route_data(u_lat, u_lon, h['latitude'], h['longitude'])
            
            # Scores
            p_score = max(0, 10 - (dist / 2)) # Proximity
            cost = h['estimated_cost']
            a_score = max(0, 10 - (cost / 20000)) # Affordability
            q_score = (h['google_rating'] * 2) # Quality
            
            # Weighted Score: 40% Quality, 30% Distance, 30% Price
            total = (0.4 * q_score) + (0.3 * p_score) + (0.3 * a_score)
            
            h['distance_km'] = dist
            h['time_car'] = time
            h['time_bike'] = int(time * 0.7)
            h['total_score'] = round(total, 1)
            
            processed.append(h)

        return sorted(processed, key=lambda x: x['total_score'], reverse=True)