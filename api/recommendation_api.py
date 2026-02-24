import json
import asyncio
from common.redis_client import get_redis_client
from fastapi import APIRouter, HTTPException
from openai import OpenAI

from common.config import get_settings
from common.db import get_db_connection

settings = get_settings()
router = APIRouter(tags=["recommendations"])

client = OpenAI(api_key=settings.openai_api_key) if settings.openai_api_key else None


@router.get("/recommendations/{user_id}")
async def get_recommendations(user_id: int):
    """Get AI-powered movie recommendations for user"""
    redis_client = get_redis_client()
    db = get_db_connection()
    cursor = db.cursor()
    
    # Get user's top movies from aggregates
    cursor.execute(
        "SELECT top_movies FROM user_aggregates WHERE user_id = %s",
        (user_id,)
    )
    result = cursor.fetchone()
    
    if not result or not result[0]:
        cursor.close()
        raise HTTPException(status_code=404, detail="User not found")
    
    top_movies = result[0]
    db_updated_at = int(result[1].timestamp())
    
    # Check cache with timestamp validation
    cache_key = f"recs:{user_id}"
    cached = redis_client.get(cache_key)
    
    if cached:
        cached_data = json.loads(cached)
        cache_timestamp = cached_data.get("cached_at", 0)
        
        # If cache is newer than or equal to DB update, use cache
        if cache_timestamp >= db_updated_at:
            cursor.close()
            return cached_data
    
    # Get genres for user's top movies
    cursor.execute(
        "SELECT genres FROM movies WHERE movie_id = ANY(%s)",
        (top_movies,)
    )
    rows = cursor.fetchall()
    all_genres = []
    for row in rows:
        if row[0]:
            all_genres.extend(row[0].split('|'))
    
    
    from collections import Counter
    top_genres = [g for g, _ in Counter(all_genres).most_common(3)]
    
    # Get all movies from database
    cursor.execute("SELECT movie_id, title, genres FROM movies LIMIT 200")
    all_movies = [
        {"movie_id": row[0], "title": row[1], "genres": row[2]}
        for row in cursor.fetchall()
    ]
    cursor.close()
    
    # AI to recommend movies
    prompt = f"""Based on this user's preferences, recommend 5 movies from the list below.

    User Info:
    - Favorite Genres: {', '.join(top_genres)}
    - User has watched {len(top_movies)} movies

    Available Movies:
    {json.dumps(all_movies, indent=2)}

    Return ONLY a JSON array of 5 movie_ids, like: [1, 2, 3, 4, 5]"""
    recommendations = await ai(prompt=prompt, all_movies=all_movies)
    
    
    
    return {
        "user_id": user_id,
        "recommendations": recommendations,
        "based_on_genres": top_genres,
        "user_top_movies": top_movies
    }


async def ai(prompt, all_movies):
    # If no API key, deterministic fallback.
    if not client:
        return all_movies[:5]

    def call():
        resp = client.responses.create(
            model="gpt-5.2",
            input=prompt,
            temperature=0.7,
        )
        return resp.output_text

    text = await asyncio.to_thread(call)
    try:
        recommended_ids = json.loads(text)
    except Exception:
        # if the model returns plain text, fall back to first 5
        return all_movies[:5]

    return [m for m in all_movies if m["movie_id"] in recommended_ids]
