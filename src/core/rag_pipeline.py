from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
import os

class RAGPipeline:
    def __init__(self):
        self.embeddings = OpenAIEmbeddings()
        self.vector_store = None

    def initialize_store(self, texts, metadatas=None):
        """
        Initialize the FAISS vector store with specific texts.
        """
        if not texts:
            return {"error": "No texts provided"}
            
        docs = [Document(page_content=t, metadata=m or {}) for t, m in zip(texts, metadatas or [{}]*len(texts))]
        self.vector_store = FAISS.from_documents(docs, self.embeddings)
        return {"status": "Vector Store Initialized", "count": len(texts)}

    def search(self, query, k=3):
        """
        Search for relevant context.
        """
        if not self.vector_store:
            return {"error": "Vector store not initialized"}
            
        results = self.vector_store.similarity_search(query, k=k)
        return [{"content": d.page_content, "metadata": d.metadata} for d in results]

    def save_index(self, path="faiss_index"):
        if self.vector_store:
            self.vector_store.save_local(path)

    def load_index(self, path="faiss_index"):
        if os.path.exists(path):
            self.vector_store = FAISS.load_local(path, self.embeddings, allow_dangerous_deserialization=True)
            return True
        return False

rag_pipeline = RAGPipeline()
