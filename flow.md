     NETFLIX RECOMMENDATION SYSTEM FLOW                


──────────────────────────────────────────────────────────────
 PHASE 1: DATA INGESTION
──────────────────────────────────────────────────────────────

ratings.csv  ──▶  API  ──▶  Kafka  ──▶  S3
                                         
movies.csv   ──▶  API  ──▶  PostgreSQL  
                             (movies)    

──────────────────────────────────────────────────────────────
 PHASE 2: DAILY AGGREGATION (Airflow)
──────────────────────────────────────────────────────────────

S3 (raw events)  ──▶  Aggregation Script  ──▶  PostgreSQL
                                            (user_aggregates)
                                                      │
                                                      ▼
                                                   Redis
                                              (invalidate cache)

──────────────────────────────────────────────────────────────
 PHASE 3: RECOMMENDATIONS (Real-Time)
──────────────────────────────────────────────────────────────

User Request  ──▶  Check Redis
                      │
                 ┌────┴────┐
              MISS         HIT
                │           │
                ▼           ▼
           PostgreSQL    Return
           (get user     Cached
            preferences) Result
                │
                ▼
           OpenAI API
           (generate 5
            recommendations)
                │
                ▼
           Cache in Redis
                │
                ▼
           Return to User


──────────────────────────────────────────────────────────────
 KEY COMPONENTS
──────────────────────────────────────────────────────────────

Storage:      PostgreSQL, S3, Redis
Streaming:    Kafka
Orchestration: Airflow (daily batch)
AI:           OpenAI API
APIs:         FastAPI
DB UI:        Adminer (localhost:8081)
