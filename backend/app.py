from flask import Flask, request, jsonify
from flask_cors import CORS
from services.nlp_service import NlpService
from services.ranking_service import RankingService
from models.database import Database

app = Flask(__name__)
CORS(app)
db = Database()
nlp = NlpService('../data/knowledge_base.csv')
ranker = RankingService(db)

@app.route('/api/recommend', methods=['POST'])
def rec():
    d = request.json
    tid, name, score = nlp.classify_disease(d.get('query'))
    if not tid: return jsonify({"error": "Unknown condition"}), 404
    return jsonify({"disease_detected": name, "hospitals": ranker.rank_hospitals(tid, d.get('latitude'), d.get('longitude'))})

if __name__ == '__main__': app.run(debug=True, port=5000)