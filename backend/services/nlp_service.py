from sentence_transformers import SentenceTransformer, util
import pandas as pd
import torch

class NlpService:
    def __init__(self, kb_path):
        print("  Loading AI Model...")
        self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        
        # Load Knowledge Base
        self.kb = pd.read_csv(kb_path)
        
        # Clean keywords for exact matching (lowercase, strip spaces)
        self.kb['clean_keywords'] = self.kb['keywords'].apply(lambda x: x.lower().strip())
        
        # Pre-compute Embeddings for AI Fallback
        self.kb['search_text'] = self.kb['disease_name_english'] + " " + self.kb['keywords']
        self.embeddings = self.model.encode(self.kb['search_text'].tolist(), convert_to_tensor=True)
        print("  AI Ready & Keywords Indexed.")

    def classify_disease(self, query):
        if not query: return None, None, 0.0
        
        query_clean = query.lower().strip()

        # --- STEP 1: EXACT KEYWORD MATCH (The "Dumb but Fast" Check) ---
        # If the user types a phrase that exists exactly in our CSV, pick it immediately.
        for index, row in self.kb.iterrows():
            # Check if the query appears inside the keywords list
            keywords_list = [k.strip() for k in row['clean_keywords'].split(',')]
            
            # Check 1: Is the query exactly one of the keywords?
            if query_clean in keywords_list:
                print(f"  [Exact Match] '{query}' -> {row['disease_name_english']}")
                return int(row['treatment_id']), row['disease_name_english'], 1.0
            
            # Check 2: Is the query PART of the keywords? (e.g. "pitt ki thaili" in "pitt ki thaili mein pathri")
            for k in keywords_list:
                if query_clean in k or k in query_clean:
                    # Verify it's a significant match (not just "dard")
                    if len(query_clean) > 4: 
                        print(f"  [Partial Match] '{query}' -> {row['disease_name_english']}")
                        return int(row['treatment_id']), row['disease_name_english'], 0.95

        # --- STEP 2: AI SEMANTIC MATCH (The "Smart" Fallback) ---
        print("  No exact match found. Asking AI...")
        query_emb = self.model.encode(query, convert_to_tensor=True)
        scores = util.cos_sim(query_emb, self.embeddings)[0]
        
        best_idx = torch.argmax(scores).item()
        best_score = scores[best_idx].item()
        
        matched_disease = self.kb.iloc[best_idx]['disease_name_english']
        print(f"  [AI Guess] '{query}' -> {matched_disease} (Score: {round(best_score, 2)})")

        if best_score < 0.25: 
            return None, "Unknown Condition", 0.0
        
        return int(self.kb.iloc[best_idx]['treatment_id']), matched_disease, best_score
    