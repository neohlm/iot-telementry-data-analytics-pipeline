"""
IoT Sensor Data Simulator
Continuously sends sensor events to Azure Event Hubs every 30 seconds.
"""

import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from typing import List

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# ─── Configuration ────────────────────────────────────────────────────────────

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://iottelementrynamespace.servicebus.windows.net/;SharedAccessKeyName=IoTTelementryJob_iottelementryevents_policy;SharedAccessKey=r33UtkbiIDCjFqnTYk4Xp3FA1FaSz5q+Y+AEhGjK0Gc=;EntityPath=iottelementryevents"
EVENT_HUB_NAME = "iottelementryevents"

# Sensor topology
ROOMS = ["room-101", "room-102", "room-103", "room-201", "room-202", "lobby", "server-room"]
SENSORS_PER_ROOM = 2

# Simulation parameters
TOTAL_NORMAL_EVENTS = 50
BURST_SENSOR_COUNT = 2
BURST_SIZE = 15
DUPLICATE_RATIO = 0.10
LATE_ARRIVAL_COUNT = 8
LATE_ARRIVAL_MAX_SECONDS = 300

# Temperature / humidity ranges per room type
ROOM_PROFILES = {
    "server-room": {"temp_range": (18.0, 24.0), "humidity_range": (40.0, 55.0)},
    "lobby":       {"temp_range": (20.0, 26.0), "humidity_range": (35.0, 60.0)},
    "default":     {"temp_range": (19.0, 25.0), "humidity_range": (30.0, 65.0)},
}

SEND_BATCH_SIZE = 20
INTER_EVENT_DELAY_S = 0.05
LOOP_INTERVAL_S = 30  # seconds between simulation runs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Data Model ───────────────────────────────────────────────────────────────

@dataclass
class SensorEvent:
    eventId: str
    sensorId: str
    roomId: str
    eventTime: str
    temperature: float
    humidity: float
    _meta: dict = None

    def to_payload(self) -> dict:
        d = asdict(self)
        d.pop("_meta", None)
        return d


# ─── Sensor Registry ──────────────────────────────────────────────────────────

def build_sensor_registry() -> dict:
    registry = {}
    for room in ROOMS:
        for i in range(1, SENSORS_PER_ROOM + 1):
            sensor_id = f"sensor-{room}-{i:02d}"
            registry[sensor_id] = room
    return registry


SENSOR_REGISTRY = build_sensor_registry()
ALL_SENSOR_IDS = list(SENSOR_REGISTRY.keys())


# ─── Event Factory ────────────────────────────────────────────────────────────

def _room_profile(room_id: str) -> dict:
    return ROOM_PROFILES.get(room_id, ROOM_PROFILES["default"])


def generate_event(
    sensor_id: str,
    event_time: datetime = None,
    existing_event_id: str = None,
    label: str = "normal",
) -> SensorEvent:
    room_id = SENSOR_REGISTRY[sensor_id]
    profile = _room_profile(room_id)
    temperature = round(random.uniform(*profile["temp_range"]), 2)
    humidity = round(random.uniform(*profile["humidity_range"]), 2)
    ts = event_time or datetime.now(timezone.utc)

    return SensorEvent(
        eventId=existing_event_id or str(uuid.uuid4()),
        sensorId=sensor_id,
        roomId=room_id,
        eventTime=ts.isoformat(),
        temperature=temperature,
        humidity=humidity,
        _meta={"label": label},
    )


# ─── Event Stream Generators ──────────────────────────────────────────────────

def generate_normal_events(count: int) -> List[SensorEvent]:
    log.info("Generating %d normal events ...", count)
    return [
        generate_event(random.choice(ALL_SENSOR_IDS), label="normal")
        for _ in range(count)
    ]


def inject_burst_events(burst_size: int = BURST_SIZE) -> List[SensorEvent]:
    sensor = random.choice(ALL_SENSOR_IDS)
    log.info("Burst: %d events from %s", burst_size, sensor)
    now = datetime.now(timezone.utc)
    return [
        generate_event(
            sensor,
            event_time=now + timedelta(milliseconds=i * 20),
            label="burst",
        )
        for i in range(burst_size)
    ]


def inject_duplicates(source_events: List[SensorEvent], ratio: float = DUPLICATE_RATIO) -> List[SensorEvent]:
    sample = random.sample(source_events, k=max(1, int(len(source_events) * ratio)))
    log.info("Duplicating %d events", len(sample))
    return [
        generate_event(
            e.sensorId,
            existing_event_id=e.eventId,
            label="duplicate",
        )
        for e in sample
    ]


def inject_late_arrivals(
    count: int = LATE_ARRIVAL_COUNT,
    max_late_seconds: int = LATE_ARRIVAL_MAX_SECONDS,
) -> List[SensorEvent]:
    log.info("Generating %d late-arrival events", count)
    now = datetime.now(timezone.utc)
    events = []
    for _ in range(count):
        delay = random.randint(30, max_late_seconds)
        past_time = now - timedelta(seconds=delay)
        sensor = random.choice(ALL_SENSOR_IDS)
        events.append(generate_event(sensor, event_time=past_time, label="late"))
    return events


# ─── Event Stream Builder ─────────────────────────────────────────────────────

def build_event_stream() -> List[SensorEvent]:
    normal = generate_normal_events(TOTAL_NORMAL_EVENTS)

    burst_events: List[SensorEvent] = []
    for _ in range(BURST_SENSOR_COUNT):
        burst_events.extend(inject_burst_events())

    duplicates = inject_duplicates(normal)
    late = inject_late_arrivals()

    all_events = normal + burst_events + duplicates + late
    random.shuffle(all_events)

    log.info(
        "Stream built: %d normal | %d burst | %d duplicate | %d late  ->  %d total",
        len(normal), len(burst_events), len(duplicates), len(late), len(all_events),
    )
    return all_events


# ─── Azure Event Hubs Sender ──────────────────────────────────────────────────

async def send_events(events: List[SensorEvent]) -> None:
    log.info("Connecting to Event Hub '%s' ...", EVENT_HUB_NAME)

    async with EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    ) as producer:

        sent_total = 0
        batch_num = 0

        for chunk_start in range(0, len(events), SEND_BATCH_SIZE):
            chunk = events[chunk_start: chunk_start + SEND_BATCH_SIZE]

            # partition_key passed to create_batch — correctly routes to EH partition
            event_data_batch = await producer.create_batch(
                partition_key=chunk[0].sensorId
            )

            for evt in chunk:
                payload = json.dumps(evt.to_payload()).encode("utf-8")
                ed = EventData(payload)

                try:
                    event_data_batch.add(ed)
                except ValueError:
                    # Batch full — send it and start a new one
                    await producer.send_batch(event_data_batch)
                    sent_total += len(event_data_batch)
                    event_data_batch = await producer.create_batch(
                        partition_key=evt.sensorId
                    )
                    event_data_batch.add(ed)

            await producer.send_batch(event_data_batch)
            batch_num += 1
            sent_total += len(chunk)
            log.info("  Batch %d sent — cumulative: %d / %d events", batch_num, sent_total, len(events))
            await asyncio.sleep(INTER_EVENT_DELAY_S)

    log.info("Done — %d events sent to Event Hub.", sent_total)


# ─── Dry-run Preview ──────────────────────────────────────────────────────────

def dry_run_preview(events: List[SensorEvent], sample: int = 5) -> None:
    label_counts: dict = {}
    for e in events:
        lbl = (e._meta or {}).get("label", "unknown")
        label_counts[lbl] = label_counts.get(lbl, 0) + 1

    print("\n-- Event mix --------------------------------------------------")
    for lbl, cnt in sorted(label_counts.items()):
        print(f"  {lbl:<12}  {cnt:>4} events")
    print(f"  {'TOTAL':<12}  {len(events):>4} events")

    print(f"\n-- Sample ({sample}) -------------------------------------------")
    for e in random.sample(events, min(sample, len(events))):
        lbl = (e._meta or {}).get("label", "?")
        print(f"  [{lbl:>9}]  {e.sensorId:<28} {e.eventTime}  "
              f"T={e.temperature}C  H={e.humidity}%")
    print()


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main(dry_run: bool = False) -> None:
    if dry_run:
        events = build_event_stream()
        dry_run_preview(events)
        log.info("Dry-run complete — no events sent to Azure.")
        return

    # Continuous loop — keeps sending every 30s so ASA always has live data
    log.info("Starting continuous simulation — press Ctrl+C to stop.")
    run = 0
    while True:
        run += 1
        log.info("=== Run %d ===", run)
        events = build_event_stream()
        await send_events(events)
        log.info("Sleeping %ds before next run ...", LOOP_INTERVAL_S)
        await asyncio.sleep(LOOP_INTERVAL_S)


if __name__ == "__main__":

    asyncio.run(main(dry_run=False))
 