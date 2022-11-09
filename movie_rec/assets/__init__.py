from dagster import asset, ScheduleDefinition, job, op
from kafka import KafkaConsumer


@asset
def events():
    consumer = KafkaConsumer("movielog10", group_id="dagster_demo")
    watch_events = []
    recommend_events = []
    while len(watch_events) < 100 or len(recommend_events) < 100:
        value = next(consumer).value.decode()
        try:
            if "/data/m" in value:
                watch_events.append(_parse_watch_event(value))
            elif "recommendation request" in value:
                recommend_events.append(_parse_recommendation_event(value))
        except Exception:
            continue
    return watch_events, recommend_events


@op
def calc_hit_rate(events):
    print(events)


def _parse_watch_event(value: str) -> dict:
    _PREFIX = "GET /data/m/"
    _SUFFIX = ".mpg"
    segments = value.split(",")
    _, user_id, movie_id_and_minute = [segment.strip() for segment in segments]
    movie_id_and_minute = movie_id_and_minute[len(_PREFIX) : -len(_SUFFIX)]
    movie_id_and_minute_segments = movie_id_and_minute.split("/")
    return {
        "user_id": int(user_id),
        "movie_id": movie_id_and_minute_segments[0].strip(),
    }


def _parse_recommendation_event(value: str) -> dict:
    segments = value.split(",")
    _time = segments.pop(0).strip()
    user_id = int(segments.pop(0).strip())
    segments.pop(0)  # recommendation request...
    _status = int(segments.pop(0).strip()[len("status ") :])
    _latency_ms = int(segments.pop().strip().split()[0])
    segments[0] = segments[0].strip()[len("result: ") :]
    result = [item.strip() for item in segments]
    return {"user_id": user_id, "movies": result}
