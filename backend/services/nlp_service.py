from sentence_transformers import SentenceTransformer, util
import pandas as pd
import torch

class NlpService:
    def __init__(self, kb_path):
        print("Loading AI...")
        self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        self.kb = pd.read_csv(kb_path)
        self.embs = self.model.encode((self.kb['disease_name_english']+" "+self.kb['keywords']).tolist(), convert_to_tensor=True)

    def classify_disease(self, query):
        if not query: return None, None, 0
        scores = util.cos_sim(self.model.encode(query, convert_to_tensor=True), self.embs)[0]
        best = torch.argmax(scores).item()
        if scores[best] < 0.25: return None, None, 0
        return int(self.kb.iloc[best]['treatment_id']), self.kb.iloc[best]['disease_name_english'], scores[best].item()