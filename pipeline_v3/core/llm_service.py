import logging
import os
import google.generativeai as genai
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Hardcoded for the moment as requested
GEMINI_KEY = "AIzaSyAx6yHIS0h1KiVI96yscSx-dSmB238WCao"

class LLMService:
    """
    Bridge to LLM providers (Gemini) for financial text analysis.
    Used for Concall summaries and Sentiment scoring.
    """
    
    def __init__(self, provider: str = "gemini"):
        self.provider = provider
        self.api_key = GEMINI_KEY
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-pro-latest')
        else:
            self.model = None

    def analyze_concall(self, title: str, context: str = "") -> Dict[str, Any]:
        """
        Summarizes a concall transcript and extracts sentiment.
        For now, we use the title and meta, as we don't download the full PDF mid-pipeline yet.
        """
        if not self.api_key or not self.model:
            return self._rule_based_analysis(title + " " + context)

        prompt = f"""
        You are an institutional financial analyst. 
        Analyze this corporate announcement: "{title}"
        Context: {context}
        
        Provide a JSON response with:
        1. "sentiment": One of "Positive", "Neutral", "Cautionary"
        2. "summary": A one-sentence summary of management's tone and primary message.
        3. "highlights": List of 3 key takeaways.
        
        Example JSON:
        {{
            "sentiment": "Positive",
            "summary": "Management raised FY25 guidance citing strong traction in retail and digital.",
            "highlights": ["15% YoY EBITDA growth", "Debt reduction on track", "New product launches"]
        }}
        """

        try:
            response = self.model.generate_content(prompt)
            # Try to extract JSON from response text
            import json
            import re
            match = re.search(r'\{.*\}', response.text, re.DOTALL)
            if match:
                return json.loads(match.group())
            return {"sentiment": "Neutral", "summary": response.text[:200]}
        except Exception as e:
            logger.error(f"Gemini analysis failed: {e}")
            return self._rule_based_analysis(title)

    def _rule_based_analysis(self, text: str) -> Dict[str, Any]:
        """Simple keyword-based sentiment for when LLM is unavailable."""
        pos_words = ["growth", "record", "optimistic", "strong", "expansion", "dividend", "reduction", "positive", "transcript"]
        neg_words = ["challenge", "decline", "pressure", "headwind", "loss", "impact", "caution", "weak", "delayed"]
        
        text_lower = text.lower()
        pos_count = sum(1 for w in pos_words if w in text_lower)
        neg_count = sum(1 for w in neg_words if w in text_lower)
        
        score = pos_count - neg_count
        sentiment = "Positive" if score > 5 else "Cautionary" if score < -2 else "Neutral"
        
        return {
            "sentiment": sentiment,
            "summary": f"Automated analysis detected {pos_count} positive and {neg_count} cautionary signals in the transcript.",
            "mode": "rule-based"
        }
