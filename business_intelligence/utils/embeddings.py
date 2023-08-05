import requests
from constants import HF_API_KEY

model_id = "thenlper/gte-base"
api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"
headers = {"Authorization": f"Bearer {HF_API_KEY}"}


def emb_text(text: str) -> list:
    """Calcultate the embeddings of the input text"""
    response = requests.post(api_url, headers=headers, json={"inputs": text, "options": {"wait_for_model": True}})
    return response.json()


