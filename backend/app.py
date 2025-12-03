from flask import Flask, request, jsonify
from flask_cors import CORS
from services.nlp_service import NlpService
from services.ranking_service import RankingService
from models.database import Database

app = Flask(__name__)
# Enable CORS for all routes and origins to fix connection errors
CORS(app, resources={r"/*": {"origins": "*"}})

print("--- Initializing Backend ---")
db = Database()
# Load NLP model immediately to catch errors early
try:
    nlp = NlpService('../data/knowledge_base.csv')
    print("NLP Service Ready.")
except Exception as e:
    print(f"CRITICAL ERROR loading NLP: {e}")

ranker = RankingService(db)

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "active", "message": "HealthNet AI Server is Running"})

@app.route('/api/recommend', methods=['POST'])
def rec():
    try:
        d = request.json
        if not d: return jsonify({"error": "No data received"}), 400
        
        query = d.get('query')
        lat = d.get('latitude')
        lon = d.get('longitude')
        
        print(f"Received Query: {query} at ({lat}, {lon})")

        tid, name, score = nlp.classify_disease(query)
        
        if not tid: 
            print("NLP failed to classify.")
            return jsonify({"error": "Could not understand symptoms. Try 'fever', 'pain', etc."}), 404
        
        print(f"Classified as: {name}")
        res = ranker.rank_hospitals(tid, lat, lon)
        
        return jsonify({
            "disease_detected": name, 
            "hospitals": res,
            "count": len(res)
        })
        
    except Exception as e:
        print(f"SERVER ERROR: {e}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

if __name__ == '__main__':
    # Run on 0.0.0.0 to ensure it's accessible externally if needed
    app.run(debug=True, port=5000, host='0.0.0.0')