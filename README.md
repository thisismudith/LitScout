# 📘LitScout

**LitScout** is a Python-based platform that acts as your **personal research companion**. It helps you discover, analyze, and plan your research journey — from finding relevant papers to exploring top conferences, authors, and publishing opportunities. 

LitScout consists of:
- A **Flask Frontend** for user interaction and visualization.
- A **Python 3.11 Backend** for data processing, NLP analysis, and API management.

---

## 🚀 Project Overview


### **❓ The Problem**
Research discovery today is:

- Keyword dependent  
- Fragmented across platforms  
- Time-consuming  
- Biased toward popularity rather than relevance  

Most tools show results — not **insights**.

### **⚡ The Approach**

LitScout uses ML to create a unified research ecosystem that enables users to:

- Search and summarize the latest research papers *(based upon lexical context and section priority)*.  
- Explore **authors** and other sources of **collaboration networks**.  
- Discover and compare **academic conferences** and **journals**.  
- Upload a paper to find **where to publish** and view **conference suitability scores**.  

---

## 🧩 Project Architecture

LitScout follows an architecture with **two sub-sections**:

### **1. Backend (FastAPI-like Structure)**
- Handles all data processing, ML, and API logic.
- Exposes endpoints that the frontend can interact with.
- Manages integrations with the OpenAlex API.

### **2. Frontend (Flask)**
- Provides a modern, responsive UI for users.
- Fetches processed data from the backend.
- Displays papers, author stats, conference data, and recommendations through interactive components and dashboards.

---

## ⚙️ Main Components

| Component | Description |
|------------|--------------|
| **`/server/ingest/`** | Fetches and normalizes data from APIs like Semantic Scholar, arXiv, Crossref, OpenAlex, and WikiCFP. |
| **`/server/db/`** | Database models and storage layer (SQLite/PostgreSQL) for papers, authors, embeddings, clusters, and conferences. |
| **`/server/embed/`** | Generates embeddings (numerical meaning representations) for abstracts, author bios, and uploaded papers using Sentence Transformers. |
| **`/server/concepts/`** | Uses OpenAlex concept weightage and performs embedding. |
| **`/server/search/`** | Search feature for papers, authors, concepts, and conferences. |
| **`/client/`** | Flask components for Dashboard, Author View, Conference Explorer, and Paper Recommendation interface. |
| **`/client/src/services/`** | API integration layer to communicate with FastAPI backend. |
| **`/client/templates/`** | Routes for the html code. |

---

## 🔄 Data Flow

1. **User Action**
   - The user enters a topic or uploads a research paper in the React interface.  
2. **API Request**
   - Flask sends a request to the API backend using HTML request endpoints.  
3. **Data Collection**
   - API retrieves relevant papers, authors, and conferences from OpenAlex.  
4. **Processing**
   - Papers are embedded using bge-base-en-v1.5 and clustered into research themes.  
   - Summaries, keywords, and trend analyses are generated for each cluster.  
5. **Author & Conference Analytics**
   - Backend computes author metrics and conference metrics. 
6. **Response to Frontend**
   - API sends structured JSON data to the Flask app.  
7. **Visualization**
   - Flask renders the data.

---

## 🧰 Tech Stack

| Category | Tools / Frameworks |
|-----------|------------------|
| **Frontend** | Flask, HTML, CSS, Vanilla JS |
| **Backend** | ML Model (Python 3.11), Uvicorn (server), Postgres (database) |
| **ML Libraries** | Sentence-Transformers, Transformers, pgvectors |
| **Database** | PostgreSQL |
| **External APIs** | OpenAlex API |
| **Version Control** | Git & GitHub for code and documentation |
| Parallelism | ThreadPoolExecutor |

---

## 🖥️ Internal Logic

### **1️⃣ Query or Uploaded Paper → Embedding**
The user input (text or extracted PDF content) is encoded

### **2️⃣ Hybrid Paper Scoring**

#### 🟣 Direct vector similarity  
`paper_score_direct = cosine_similarity(query_vector, paper_embedding)`

#### 🔵 Concept-weighted similarity
Each concept has its own embedding and weight within a paper:
`concept_score = avg(similarity(query, concept) × weight)`

##### 🟢 Final Score:
`paper_score = α × paper_score_direct + (1 - α) × concept_score`

### **3️⃣ Author Scoring (Order-Weighted)**
author_order approximates contribution:
`author_weight = 1 / author_order`

`author_score = Σ(final_paper_score × author_weight)`

More aligned → higher score.

---

### **5️⃣ UI & Response Handling**

- First results load instantly  
- Next results preload **before user scrolls**  
- Pagination is **cached**, not re-run  
- Panels scroll independently (page never scrolls)

---

### 🛠️ Key Challenges & Solutions

| Challenge                   | Solution                                                   |
| --------------------------- | ---------------------------------------------------------- |
| Keyword-based irrelevance   | Hybrid semantic + concept matching                         |
| API rate limits             | Threaded ingestion with retry + batching                   |
| Missing structured metadata | Custom OpenAlex normalization layer                        |
| UI sluggishness             | Prefetch-based pagination + client caching                 |
| Fair author scoring         | Formula based on `contribution order × semantic relevance` |

---

## 📝 Future Improvements
- Add user accounts for saving searches and preferences.
- Integrate more data sources (e.g., PubMed, IEEE Xplore).
- Implement advanced visualization (e.g., network graphs for authors).
- Enhance NLP summaries with more context-aware models.
- Use an ML model (Random Forest / XGBoost) for adding more features such as timeline, history, credibility, and citations.
