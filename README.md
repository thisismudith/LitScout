#  📘LitScout

**LitScout** is a Python-based platform that acts as your **personal research companion**. It helps you discover, analyze, and plan your research journey — from finding relevant papers to exploring top conferences, authors, and publishing opportunities. 

LitScout consists of:
- A **React.js Frontend** for user interaction and visualization.
- A **FastAPI Backend** for data processing, AI/NLP analysis, and API management.

---

##  🚀 Project Overview

LitScout combines **Artificial Intelligence (AI)**, **Machine Learning (ML)**, and **Natural Language Processing (NLP)** to create a unified research ecosystem that enables users to:

- Search and summarize the latest research papers *(based upon lexical context and section priority)*.  
- Explore **authors** and other sources of **collaboration networks**.  
- Discover and compare **academic conferences** and **journals**.  
- Upload a paper to find **where to publish** and view **conference suitability scores**.  
- Receive a **step-by-step publishing guide** including submission timelines and next steps after selecting the "where to publish."

---

##  🧩 Project Architecture

LitScout follows an architecture with **two sub-sections**:

###  **1. Backend (FastAPI)**
- Handles all data processing, ML, and API logic.
- Exposes endpoints that the React frontend can interact with.
- Manages integrations with research APIs (Semantic Scholar, arXiv, Crossref, OpenAlex, WikiCFP, etc.).

###  **2. Frontend (React.js)**
- Provides a modern, responsive UI for users.
- Fetches processed data from the FastAPI backend.
- Displays papers, clusters, author stats, conference data, and recommendations through interactive components and dashboards.

---

##  ⚙️ Main Components

| Component | Description |
|------------|--------------|
| **`/server/ingest/`** | Fetches and normalizes data from APIs like Semantic Scholar, arXiv, Crossref, OpenAlex, and WikiCFP. |
| **`/server/store/`** | Database models and storage layer (SQLite/PostgreSQL) for papers, authors, embeddings, clusters, and conferences. |
| **`/server/nlp/`** | Text cleaning, summarization, keyword extraction, and similarity scoring using NLP. |
| **`/server/embed/`** | Generates embeddings (numerical meaning representations) for abstracts, author bios, and uploaded papers using Sentence Transformers. |
| **`/server/cluster/`** | Performs topic clustering using UMAP (Uniform Manifold Approximation and Projection) and HDBSCAN (Hierarchical Density-Based Clustering). |
| **`/server/recommend/`** | Suggests conferences and collaborators by comparing topic embeddings and trends. |
| **`/server/conferences/`** | Stores and ranks conferences by topic relevance, deadline, and quality. |
| **`/server/authors/`** | Builds author profiles and collaboration networks using citation data and co-authorship metrics. |
| **`/client/src/components/`** | React components for Dashboard, Author View, Conference Explorer, and Paper Recommendation interface. |
| **`/client/src/services/`** | API integration layer to communicate with FastAPI backend. |
| **`/client/src/pages/`** | Routes for major pages: Discover, Authors, Conferences, and Publish. |

---

##  🔄 Data Flow

1. **User Action**
   - The user enters a topic or uploads a research paper in the React interface.  
2. **API Request**
   - React sends a request to the FastAPI backend using RESTful API endpoints.  
3. **Data Collection**
   - FastAPI retrieves relevant papers, authors, and conferences from APIs like Semantic Scholar, OpenAlex, and WikiCFP.  
4. **Processing**
   - Papers are embedded using a sentence transformer and clustered into research themes.  
   - Summaries, keywords, and trend analyses are generated for each cluster.  
5. **Author & Conference Analytics**
   - Backend computes author metrics (citations, Hirsch-index, collaboration graph) and conference metrics (ranking, deadlines, suitability).  
6. **Response to Frontend**
   - FastAPI sends structured JSON data to the React app.  
7. **Visualization**
   - React renders charts, summaries, and recommendations interactively using Plotly and D3.js.  
8. **Export / Reports**
   - The user can download a PDF “Research Guide” directly from the dashboard.

---

##  📊 Module Planning (6-Week Development Roadmap)

| Week | Focus | Key Deliverables |
|------|--------|------------------|
| **1** | Planning & Setup | Define data sources, architecture diagrams, FastAPI project setup, and React scaffold. |
| **2** | Backend Data Collection | Integrate Semantic Scholar, arXiv, Crossref, OpenAlex, and WikiCFP APIs; clean and store data. |
| **3** | AI/NLP & Clustering | Build embeddings, summarization, and topic clustering logic in FastAPI. Train AI models.|
| **4** | Frontend Development | Create React dashboards for Papers, Authors, Conferences, and Publishing suggestions. |
| **5** | Recommendation Engine | Connect “Upload a Paper” feature to backend; return best-fit conferences and collaborators. |
| **6** | Integration & Project Completion | Combine frontend and backend, refine UI/UX, deploy FastAPI + React, and finalize documentation. |

---

##  🧰 Tech Stack

| Category | Tools / Frameworks |
|-----------|------------------|
| **Frontend** | React.js, Redux Toolkit (state management), Axios (API calls), Plotly.js / D3.js (charts) |
| **Backend** | FastAPI (Python), AI Model (Python), Uvicorn (server), SQLite (database) |
| **AI / NLP Libraries** | Sentence-Transformers, UMAP, HDBSCAN, scikit-learn, KeyBERT, Sumy, Transformers |
| **Database** | SQLite (development)  → PostgreSQL (if possible under the deadline) |
| **External APIs** | Semantic Scholar, arXiv, Crossref, OpenAlex, WikiCFP |
| **Testing** | Pytest (backend), Jest (frontend) |
| **Deployment** | GitHub Web Hosting (until something better is found) |
| **Version Control** | Git & GitHub for code and documentation |

---

##  🖥️ Dashboard Overview (Frontend Pages)

| Page | Purpose |
|------|----------|
| **Discover** | Search papers, view topic clusters, and summaries. |
| **Authors** | View leading authors, their profiles, citations, and collaboration networks. |
| **Conferences** | Explore upcoming conferences with ranking, deadlines, and topics. |
| **Publish** | Upload your research paper to get recommended conferences and publishing steps. |
| **Guide / Export** | Download a personalized “Research Guidance” PDF summarizing findings. |

---

##  📍 Key Features Summary

✅ AI-based search and clustering of research papers  
✅ Topic-wise summaries and keyword insights  
✅ Author information and collaboration networks    
✅ Paper-to-conference recommendation engine  
✅ Step-by-step publishing roadmap (subject to availability)  
✅ Downloadable “Research Guidance Report” (PDF)

---

##  📚 What Will I Learn?

- Using transformer models and testing their accuracy.
- AI-model training for finding relevant research papers.
- Creating interactive visualizations with D3.js and Plotly.


---

##  📝 Notes
- Might switch to a different database if needed (PostgreSQL).
- Deployment options may evolve based on project needs and deadlines.
- Might switch to different AI/NLP libraries if better suited for tasks.
- Might use a different frontend framework as I am not that familiar with React.js.
