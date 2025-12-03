from math import radians, sin, cos, sqrt, atan2
import requests

class RankingService:
    def __init__(self, db):
        self.db = db

    def _haversine(self, lat1, lon1, lat2, lon2):
        if not lat1 or not lon1 or not lat2 or not lon2: return 999.0
        R = 6371.0
        dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        return R * 2 * atan2(sqrt(a), sqrt(1 - a)) * 6371.0

    def _get_route_data(self, lat1, lon1, lat2, lon2):
        """
        Tries OSRM API. If it fails/times out, uses Tunable Physics Math.
        """
        # --- CONFIGURATION (Tweak these if time is wrong!) ---
        AVG_SPEED_CAR_KMPH = 30  # Increase this if time is too high
        ROAD_CURVATURE_FACTOR = 1.3 # Real roads are 30% longer than straight lines
        
        try:
            # 1. Try OSRM API (Free)
            url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
            resp = requests.get(url, timeout=1.0) # Short timeout
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get('routes'):
                    dist = round(data['routes'][0]['distance'] / 1000, 1)
                    time = int(data['routes'][0]['duration'] / 60)
                    print(f"  [API] Success: {dist}km, {time}min")
                    return dist, time
        except:
            pass
        
        # 2. Fallback: Physics Math
        dist = round(self._haversine(lat1, lon1, lat2, lon2) * ROAD_CURVATURE_FACTOR, 1)
        time = int((dist / AVG_SPEED_CAR_KMPH) * 60) + 2 
        
        print(f"  [Math] Estimate: {dist}km, {time}min")
        return dist, time

    def rank_hospitals(self, t_id, u_lat, u_lon):
        hosps = self.db.get_hospitals_by_treatment(t_id)
        if not hosps: return []

        # 1. Pre-filter (Math)
        candidates = []
        for h in hosps:
            h['straight_dist'] = self._haversine(u_lat, u_lon, h['latitude'], h['longitude'])
            candidates.append(h)
        
        # Sort and take closest 15
        top_candidates = sorted(candidates, key=lambda x: x['straight_dist'])[:15]

        processed = []
        for h in top_candidates:
            # 2. Get Accurate Data
            dist, time = self._get_route_data(u_lat, u_lon, h['latitude'], h['longitude'])
            
            # Scores
            p_score = max(0, 10 - (dist / 2)) # Proximity
            a_score = max(0, 10 - (h['estimated_cost'] / 20000)) # Price
            q_score = (h['google_rating'] * 2) # Quality
            
            # Weighted Score
            total = (0.4 * q_score) + (0.3 * p_score) + (0.3 * a_score)
            
            h['distance_km'] = dist
            h['time_car'] = time
            h['time_bike'] = int(time * 0.7) # Bike is faster in traffic
            h['total_score'] = round(total, 1)
            
            processed.append(h)

        return sorted(processed, key=lambda x: x['total_score'], reverse=True)