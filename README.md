# Netflix-Style Movie Recommendation System

A real-world recommendation engine that mimics how Netflix processes user viewing data to generate personalized movie suggestions.

## What This Project Does

Ever wondered how Netflix knows exactly what you want to watch next? This project recreates that magic using real movie rating data from Kaggle. Since we don't have live Netflix data streaming in, I built APIs that simulate how user events flow through a production system - from the moment someone rates a movie to getting personalized recommendations.

## How It Works

### The Data Pipeline

1. **Event Ingestion** - When a user rates a movie, that event needs to be captured and stored. I created an API endpoint that mimics this by reading rating data from CSV files and pushing it through Kafka (like Netflix would do with real-time events).

2. **Storage Layer** - Events from Kafka get saved to S3 for long-term storage. Think of this as the data warehouse where all the raw viewing history lives.

3. **Aggregation** - Here's where it gets interesting. An Airflow job runs daily to crunch all those raw events from S3. It figures out things like "What are User 123's favorite genres?" and "Which movies did they rate highest?" These insights get stored in Postgres for quick access.

4. **Recommendations** - When you hit the recommendations API, it grabs your viewing preferences from the database, feeds them to OpenAI, and gets back 5 movies you'll probably love - all based on the genres you watch most.

### The Tech Stack

- **Postgres** - Stores movies, user preferences, and aggregated stats
- **Kafka** - Handles the streaming event data (just like real-time systems)
- **S3 (MinIO)** - Archives all the raw rating events (Mimics s3)
- **Airflow** - Orchestrates the daily batch jobs
- **Redis** - Caches recommendations so repeat requests are lightning fast
- **OpenAI** - Powers the AI-driven movie suggestions

## Why I Built It This Way

**Bulk Operations** - Loading thousands of ratings one-by-one would be painfully slow. I batch everything - both database inserts and Kafka messages. This makes ingestion way faster, though it's still heavy work. My plan is to move this to background jobs soon so the API doesn't block.

**Redis Caching** - Generating recommendations involves database queries and AI calls, which can take a couple seconds. Once we compute recommendations for a user, Redis caches them. The next request? Milliseconds instead of seconds. The cache is smart too - it checks timestamps to make sure it's not serving stale data after aggregations update.

**Common Utilities** - I got tired of writing the same connection logic everywhere, so I created reusable functions for Redis, Kafka, and Postgres connections. Now any part of the project can just import and use them.

**Migration Scripts** - Setting up the database schema is automated. Just run the migration script and all your tables are ready to go.

## The APIs

### 1. `/ingest/ratings` - Event Ingestion
Simulates user rating events flowing through the system. Reads from CSV → pushes to Kafka → saves to S3.

### 2. `/upload/movies` - Movie Catalog
Loads the movie catalog into Postgres. This is your reference data - movie titles, genres, etc.

### 3. `/recommendations/{user_id}` - Get Recommendations
The magic endpoint. Give it a user ID and get back 5 personalized movie picks based on their viewing history.

## What's Next

The data ingestion is  asynchronous,as huge amount of data leads to sever shutdown so moved the proccessing into background task.

## Running It Locally
You'll need Docker for Kafka, Postgres, Redis, and MinIO. Set up your `.env` with database credentials and an OpenAI API key. Run migrations to create tables, then hit the ingestion endpoints to populate data. Fire up Airflow to schedule the aggregation jobs, and you're ready to start serving recommendations.

# run 
docker compose up -d