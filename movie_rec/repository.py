from collections import defaultdict
from dagster import (
    load_assets_from_package_module,
    repository,
    job,
    op,
    ScheduleDefinition,
)
from kafka import KafkaConsumer

from movie_rec import assets


@op
def consume_events():
    consumer = KafkaConsumer("movielog10", group_id="dagster_demo")
    watch_events = []
    recommend_events = []
    while len(watch_events) < 1000 or len(recommend_events) < 1000:
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
def calc_hit_rate(events: tuple[list, list]):
    watch_events, recommend_events = events
    user_recommended: dict[str, set[str]] = defaultdict(set)
    for event in recommend_events:
        user_recommended[event["user_id"]].update(event["movies"])
    user_watched: dict[str, set[str]] = defaultdict(set)
    for event in watch_events:
        user_watched[event["user_id"]].add(event["movie_id"])
    return sum(
        len(user_watched[user].intersection(recommended))
        for user, recommended in user_recommended.items()
    ) / len(user_watched)


@op
def log_result(context, result):
    context.log.info(f"hit rate: {result}")


@job
def online_eval():
    log_result(calc_hit_rate(consume_events()))


online_eval_schedule = ScheduleDefinition(job=online_eval, cron_schedule="0 0 * * *")


@repository
def movie_rec():
    return [load_assets_from_package_module(assets), online_eval, online_eval_schedule]


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
