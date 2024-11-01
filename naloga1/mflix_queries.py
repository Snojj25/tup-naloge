import os
from dotenv import load_dotenv
import certifi

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables from .env file
load_dotenv()

uri = os.getenv("MONGO_URI")
print(f"Connecting to MongoDB at {uri}")

# Create a new client and connect to the server
client = MongoClient(
    uri, server_api=ServerApi("1"), tls=True, tlsCAFile=certifi.where()
)

# Send a ping to confirm a successful connection
try:
    client.admin.command("ping")
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


db = client["sample_mflix"]
movies_collection = db["movies"]


# 1. Find movies with Tom Hanks in cast
# Iskanje filmov po igralcih - Tom Hanks
def find_movies_by_actor(actor_name):
    query = {"cast": actor_name}
    movies = movies_collection.find(query)
    count = movies_collection.count_documents(query)
    print(f"\nMovies with {actor_name}:")
    for movie in movies:
        print(f"- {movie.get('title', 'No title')}")
    print(f"Total count: {count}")


# 2. Find movies with IMDB rating > 8.0
# Iskanje po oceni IMDB večji od 8.0
def find_movies_by_rating(min_rating):
    query = {"imdb.rating": {"$gt": min_rating}}
    movies = movies_collection.find(query)
    count = movies_collection.count_documents(query)
    print(f"\nMovies with IMDB rating > {min_rating}:")
    for movie in movies:
        print(
            f"- {movie.get('title', 'No title')} (Rating: {movie.get('imdb', {}).get('rating', 'N/A')})"
        )
    print(f"Total count: {count}")


# 3. Find movies shorter than 90 minutes
# Iskanje filmov, ki trajajo manj kot 90 minut
def find_movies_by_duration(max_duration):
    query = {"runtime": {"$lt": max_duration}}
    movies = movies_collection.find(query)
    count = movies_collection.count_documents(query)
    print(f"\nMovies shorter than {max_duration} minutes:")
    for movie in movies:
        print(f"- {movie.get('title', 'No title')} ({movie.get('runtime', 'N/A')} min)")
    print(f"Total count: {count}")


# 4. Find movies with more than 100 comments
# Iskanje filmov z več kot 100 komentarji
def find_movies_by_comments(min_comments):
    try:
        comments_collection = db["comments"]

        # First, get the movie_ids that have more than min_comments
        movie_ids_with_comments = comments_collection.aggregate(
            [
                {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": min_comments}}},
            ]
        )

        # Convert to list and extract movie_ids
        movie_ids = [doc["_id"] for doc in movie_ids_with_comments]

        # Then fetch the corresponding movies
        if movie_ids:
            movies = movies_collection.find({"_id": {"$in": movie_ids}})

            print(f"\nMovies with more than {min_comments} comments:")
            for movie in movies:
                # Get the exact comment count for this movie
                comment_count = comments_collection.count_documents(
                    {"movie_id": movie["_id"]}
                )
                print(
                    f"- {movie.get('title', 'No title')} ({movie.get('year', 'N/A')}) - {comment_count} comments"
                )

            print(f"Total count: {len(movie_ids)}")
        else:
            print(f"No movies found with more than {min_comments} comments")

    except Exception as e:
        print(f"An error occurred: {e}")


# 5. Find Spanish Drama movies
# Iskanje španskih dram
def find_spanish_dramas():
    query = {"languages": "Spanish", "genres": "Drama"}
    movies = movies_collection.find(query)
    count = movies_collection.count_documents(query)
    print("\nSpanish Drama movies:")
    for movie in movies:
        print(f"- {movie.get('title', 'No title')}")
    print(f"Total count: {count}")


def main():
    # Execute all queries
    try:
        # 1. Movies with Tom Hanks
        find_movies_by_actor("Tom Hanks")

        # # 2. Movies with high IMDB rating
        find_movies_by_rating(8.0)

        # # 3. Short movies
        find_movies_by_duration(90)

        # # 4. Movies with many comments
        find_movies_by_comments(100)

        # # 5. Spanish dramas
        find_spanish_dramas()

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
