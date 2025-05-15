import requests
import psycopg2
import schedule
import time
import random
from urllib.parse import urlparse
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from threading import Thread

# Flask app setup
app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Database connection parameters (update these with your PostgreSQL details)
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Kpuneet474@",
    "host": "localhost",
    "port": "5432"
}

# News API setup
NEWS_API_KEY = "772358ad345349769a9c9f699832d931"
# NEWS_API_KEY = "70d905e28b954c98bd5d58acf51542a5"
CATEGORIES = ['headlines', 'india', 'world', 'business', 'technology', 'entertainment', 'sports', 'science', 'health']
BASE_URL = "https://newsapi.org/v2/top-headlines?category={}&apiKey={}&country={}"  # Added country parameter

# Create the news table if it doesn’t exist
def init_db():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            source_id TEXT,
            source_name TEXT NOT NULL,
            author TEXT,
            title TEXT NOT NULL,
            description TEXT,
            url TEXT UNIQUE NOT NULL,
            url_to_image TEXT,
            published_at TIMESTAMP NOT NULL,
            content TEXT,
            category TEXT NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

# Map frontend categories to NewsAPI categories and countries
def map_category(category):
    category_mapping = {
        'headlines': {'category': 'general', 'country': ''},
        'india': {'category': 'general', 'country': 'in'},  # Use country=IN for India
        'world': {'category': 'general', 'country': ''},  # No specific country for world
        'business': {'category': 'business', 'country': ''},
        'technology': {'category': 'technology', 'country': ''},
        'entertainment': {'category': 'entertainment', 'country': ''},
        'sports': {'category': 'sports', 'country': ''},
        'science': {'category': 'science', 'country': ''},
        'health': {'category': 'health', 'country': ''}
    }
    return category_mapping.get(category, {'category': 'general', 'country': ''})

# Custom queries for the India category
def get_india_queries():
    queries = [
        {
            "desc": "India Business News",
            "url": f"https://newsapi.org/v2/everything?q=business+India&language=en&sortBy=publishedAt&apiKey={NEWS_API_KEY}"
        },
        {
            "desc": "India Technology News",
            "url": f"https://newsapi.org/v2/everything?q=technology+India&language=en&sortBy=publishedAt&apiKey={NEWS_API_KEY}"
        },
        {
            "desc": "Trending Political News in India",
            "url": f"https://newsapi.org/v2/everything?q=indian+politics+OR+elections+OR+parliament+OR+BJP+OR+Congress&language=en&sortBy=publishedAt&apiKey={NEWS_API_KEY}"
        },
        {
            "desc": "India Education News",
            "url": f"https://newsapi.org/v2/everything?q=education+India+OR+college+OR+CBSE&language=en&sortBy=publishedAt&apiKey={NEWS_API_KEY}"
        }
    ]
    return queries

# Fetch news articles from NewsAPI
def fetch_news(category):
    # Special handling for India category
    if category == 'india':
        return fetch_india_news(category)
    
    mapping = map_category(category)
    api_category = mapping['category']
    country = mapping['country']
    if country:
        url = BASE_URL.format(api_category, NEWS_API_KEY, country)
    else:
        url = f"https://newsapi.org/v2/top-headlines?category={api_category}&apiKey={NEWS_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        if data.get("status") == "ok":
            return data.get("articles", [])
        else:
            print(f"API returned non-ok status: {data.get('status')}")
            return []
    except requests.RequestException as e:
        print(f"Error fetching news for {category}: {e}")
        return []

def normalize_url(url):
    """Strip query params and fragments to compare base URLs only"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

# Special fetching function for India news across different topics
def fetch_india_news(category):
    all_articles = []
    queries = get_india_queries()
    
    for query in queries:
        print(f"Fetching: {query['desc']}")
        try:
            response = requests.get(query['url'])
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "ok":
                articles = data.get("articles", [])
                print(f"Found {len(articles)} articles for {query['desc']}")
                all_articles.extend(articles)
            else:
                print(f"API returned non-ok status for {query['desc']}: {data.get('status')}")
        except requests.RequestException as e:
            print(f"Error fetching {query['desc']}: {e}")
    
    # Remove duplicates based on URL
    unique_articles = []
    urls = set()
    for article in all_articles:
        url = article.get("url")
        if url and url not in urls:
            urls.add(url)
            unique_articles.append(article)
    
    print(f"Total unique India articles: {len(unique_articles)}")
    return unique_articles


# Store news articles in PostgreSQL
def store_news(articles, category):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    for article in articles:
        cur.execute("""
            INSERT INTO news (source_id, source_name, author, title, description, url, url_to_image, published_at, content, category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """, (
            article.get("source", {}).get("id", None),
            article.get("source", {}).get("name", "Unknown Source"),
            article.get("author", "Unknown Author"),
            article.get("title", "No Title"),
            article.get("description", "No Description"),
            article.get("url", ""),
            article.get("urlToImage", None),
            article.get("publishedAt", datetime.utcnow()),
            article.get("content", "No Content"),
            category.strip().lower() # Store the frontend category
        ))
    conn.commit()
    cur.close()
    conn.close()

# Job to fetch news articles every 30 minutes
def fetch_and_store_news():
    for category in CATEGORIES:
        print(f"Fetching news for category: {category}")
        articles = fetch_news(category)
        if articles:
            store_news(articles, category)
            print(f"Stored {len(articles)} articles for {category}.")
        else:
            print(f"No articles fetched for {category}.")

# Schedule the job
def run_scheduler():
    schedule.every(0.2).minutes.do(fetch_and_store_news)
    while True:
        schedule.run_pending()
        time.sleep(1)

# Flask route to fetch news articles from the database
@app.route('/api/news', methods=['GET'])
def get_news():
    # Get pagination parameters
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('pageSize', 9, type=int)
    category = request.args.get('category', None)
    search_term = request.args.get('search', None)
    
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    # Base query
    query = "SELECT * FROM news"
    params = []
    
    # Start building WHERE clause
    where_clauses = []
    
    # Add category filter if provided
    if category:
        where_clauses.append("category = %s")
        params.append(category)
    
    # Add search filter if provided
    if search_term:
        where_clauses.append("(title ILIKE %s OR description ILIKE %s OR content ILIKE %s)")
        search_pattern = f"%{search_term}%"
        params.extend([search_pattern, search_pattern, search_pattern])
    
    # Combine WHERE clauses if any
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    # Add ordering and pagination
    query += " ORDER BY published_at DESC"
    
    # First get total count for pagination
    count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
    cur.execute(count_query, params)
    total_count = cur.fetchone()[0]
    
    # Then add limit and offset for pagination
    offset = (page - 1) * page_size
    query += " LIMIT %s OFFSET %s"
    params.extend([page_size, offset])
    
    # Execute final query
    cur.execute(query, params)
    rows = cur.fetchall()
    
    news_data = [
        {
            "id": row[0],
            "source_id": row[1],
            "source_name": row[2],
            "author": row[3],
            "title": row[4],
            "description": row[5],
            "url": row[6],
            "url_to_image": row[7],
            "published_at": row[8].isoformat(),
            "content": row[9],
            "category": row[10]
        } for row in rows
    ]
    
    cur.close()
    conn.close()

    
    # Return data with pagination info
    return jsonify({
        "articles": news_data,
        "totalResults": total_count,
        "page": page,
        "pageSize": page_size
    })

if __name__ == "__main__":
    init_db()
    fetch_and_store_news()
    scheduler_thread = Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    app.run(debug=True, host="0.0.0.0", port=5000)
