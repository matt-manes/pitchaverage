from datetime import datetime

import requests
from databased import DataBased
from printbuddies import print_in_place
from whosyouragent import get_agent
from noiftimer import Timer
from pathier import Pathier

root = Pathier(__file__).parent

dbpath = "ratings.db"
page_size = 200


def init_database():
    with DataBased(dbpath) as db:
        db.create_table(
            "ratings",
            ["artist text", "album text", "rating real", "publish_date timestamp"],
        )


def get_reviews(start: int) -> requests.Response:
    params = {
        "types": "reviews",
        "sort": "publishdate asc",
        "size": page_size,
        "start": start,
    }
    url = f"https://pitchfork.com/api/v2/search/"
    return requests.get(url, headers=get_agent(True), params=params)


def parse_review(data: dict) -> tuple[str, str, int, datetime]:
    try:
        artist = data["artists"][0]["display_name"]
    except Exception as e:
        artist = ""
    try:
        album = data["tombstone"]["albums"][0]["album"]["display_name"]
    except Exception as e:
        album = ""
    try:
        rating = float(data["tombstone"]["albums"][0]["rating"]["rating"])
    except Exception as e:
        rating = -1.0
    try:
        date = datetime.strptime(
            data["pubDate"][: data["pubDate"].rfind(".")], "%Y-%m-%dT%H:%M:%S"
        )
    except Exception as e:
        date = datetime.fromtimestamp(0)
    return (artist, album, rating, date)


def extract_ratings(data: dict) -> list[tuple[str, str, int, datetime]]:
    reviews = data["results"]["list"]
    return [(parse_review(review)) for review in reviews]


def add_ratings_to_db(ratings: list[tuple[str, str, int, datetime]]):
    with DataBased(dbpath) as db:
        new_ratings = [
            rating
            for rating in ratings
            if db.count(
                "ratings",
                {
                    "artist": rating[0],
                    "album": rating[1],
                    "rating": rating[2],
                    "publish_date": rating[3],
                },
            )
            == 0
        ]
        db.add_rows("ratings", new_ratings)


def get_average_rating() -> float:
    with DataBased(dbpath) as db:
        return round(
            db.query("SELECT avg(rating) FROM ratings where rating >= 0.0;")[0][0], 2
        )


def get_num_reviews() -> int:
    with DataBased(dbpath) as db:
        return db.count("ratings")


def get_initial_start_num() -> int:
    return get_num_reviews() - 1


def main():
    init_database()
    review_index = get_initial_start_num()
    timer = Timer().start()
    print()
    while True:
        try:
            print_in_place(
                f"Scraping review index {review_index}... {timer.elapsed_str}", True
            )
            response = get_reviews(review_index)
            try:
                reviews = response.json()
            except Exception as e:
                (root / "json_error.txt").write_text(response.text)
                raise e
            ratings = extract_ratings(reviews)
            add_ratings_to_db(ratings)
            # There are 200 reviews per page, so less than 200 means no more pages
            if len(ratings) < page_size:
                print()
                print("Scrape complete.")
                break
            review_index += page_size
        except Exception as e:
            print()
            print(e)
            input(f"Error occurred on page with review index {review_index}")
            print()
    print(
        f"Pitchfork has given an average rating of {get_average_rating()} across {get_num_reviews()} albums."
    )


if __name__ == "__main__":
    main()
