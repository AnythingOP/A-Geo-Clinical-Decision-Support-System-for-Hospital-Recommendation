from math import radians, sin, cos, sqrt, atan2

class RankingService:
    def __init__(self, db): self.db = db
    def dist(self, lat1, lon1, lat2, lon2):
        if not lat1: return 999
        R = 6371.0
        dlat, dlon = radians(lat2-lat1), radians(lon2-lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
        return R * 2 * atan2(sqrt(a), sqrt(1-a))

    def rank_hospitals(self, t_id, u_lat, u_lon):
        hosps = self.db.get_hospitals_by_treatment(t_id)
        if not hosps: return []
        processed = []
        for h in hosps:
            d = self.dist(u_lat, u_lon, h['latitude'], h['longitude'])
            score = (0.6 * (h['google_rating']*2)) + (0.4 * max(0, 10-(d/2)))
            h['distance_km'] = round(d, 1)
            h['total_score'] = round(score, 1)
            h['time_car'] = int((d/30)*60)+5
            h['time_bike'] = int((d/40)*60)+2
            processed.append(h)
        
        # 5km Logic
        s = sorted(processed, key=lambda x: x['distance_km'])
        r5 = [x for x in s if x['distance_km'] <= 5]
        return sorted(r5, key=lambda x: x['total_score'], reverse=True) if r5 else s[:10]