"""JSON-only service contracts. Optional fields are omitted, never synthesized."""
from typing import Any, Literal, TypedDict


class ReadRequest(TypedDict, total=False):
    offset: int
    limit: int
    after: int
    cursor: int | str


class WriteRequest(TypedDict, total=False):
    request_id: str
    expected_revision: int
    namespace: str


class Result(TypedDict, total=False):
    status: str
    revision: int
    request_id: str
    next_offset: int | None


class PlayerRef(TypedDict, total=False):
    name: str
    xuid: str
    uuid: str
    player_id: str


class PlayerRequest(ReadRequest, WriteRequest, PlayerRef, total=False):
    player_id: str
    fields: dict[str, Any]


class MenuOption(TypedDict):
    label: str
    value: Any


class MenuStep(TypedDict, total=False):
    id: str
    prompt: str
    kind: Literal['select', 'confirm', 'text', 'integer']
    options: list[MenuOption]
    min: int
    max: int


class MenuPermission(TypedDict, total=False):
    capability: str
    scope: str
    namespace: str


class MenuDefinition(TypedDict, total=False):
    title: str
    steps: list[MenuStep]
    permission: MenuPermission


class InteractionRequest(WriteRequest, total=False):
    menu_id: str
    menu: MenuDefinition
    player: PlayerRef
    session_id: str
    value: str
    commands: list[str]
    args: list[dict[str, str]]
    ttl_seconds: int


class ObservationRequest(WriteRequest, ReadRequest, total=False):
    player: PlayerRef
    kinds: list[str]
    kind: str
    sequence: int
    subscription_id: str
    cursor: int


class BridgeRequest(WriteRequest, ReadRequest, total=False):
    route_id: str
    prefix: str
    priority: int
    consume: bool
    cursor: int


class ChunkRequest(WriteRequest, ReadRequest, total=False):
    dimension: int
    pos: list[int]
    max_age: float
    cell: list[int]
    nbt_offset: int
    length: int
    ttl: float
    subscription_id: str


class WorldRequest(WriteRequest, ReadRequest, total=False):
    task_id: str
    request_key: str
    index: int
    source_name: str
    position: list[int]
    dimension: str
    confirm_token: str
    confirmation_key: str
    kind: str
    args: dict[str, Any]
    pos: list[int]


class MusicRequest(WriteRequest, total=False):
    playback_id: str
    targets: list[str]
    midi_base64: str
    speed: float
    sound: str
    volume: float


class ScoreRequest(WriteRequest, ReadRequest, total=False):
    objective: str
    action: str
    player_id: str
    value: int
    name: str


class RoleRequest(WriteRequest, ReadRequest, total=False):
    role: str
    action: str
    player_id: str
    capabilities: list[str]
    capability: str
    scope: str
    expires_at: str | None


class ModerationRequest(WriteRequest, ReadRequest, total=False):
    player_id: str
    reason: str
    expires_at: str | None


class EconomyRequest(WriteRequest, ReadRequest, total=False):
    account: str
    currency: str
    target: str
    target_expected_revision: int
    amount: int
    hold_id: str
    capture: bool


class SchedulerRequest(WriteRequest, ReadRequest, total=False):
    schedule_id: str
    due_at: str
    interval_seconds: int
    timezone: str
    missed: Literal['coalesce', 'skip', 'catch_up']
    payload: dict[str, Any]
    trigger_id: str
    token: str
    outcome: Literal['succeeded', 'failed', 'unknown']


class MessagingRequest(WriteRequest, ReadRequest, total=False):
    route_id: str
    delivery_id: str
    text: str


class OperationReceipt(Result, total=False):
    task_id: str
    state: str
    operation_id: str
    operation_ids: list[str]
    playback_id: str
    session_id: str
    subscription_id: str
    route_id: str
    delivery_id: str
    cursor: int
    gap: bool
    events: list[dict[str, Any]]
    updates: list[dict[str, Any]]
    intent: dict[str, Any]
    error: str


class PlayerResult(Result, total=False):
    player: PlayerRef
    players: list[PlayerRef]
    items: list[dict[str, Any]]
    fields_revision: int
    names: list[dict[str, Any]]
    first_seen: str
    last_seen: str
    next_cursor: str | None
    fields: dict[str, Any]
    history: list[dict[str, Any]]


class Balance(Result, total=False):
    account: str
    currency: str
    balance: int
    held: int
    available: int
    sequence: int


class Trigger(TypedDict):
    schedule_id: str
    trigger_id: str
    token: str
    expires_at: float
    scheduled_at: float
    payload: dict[str, Any]


class SchedulerResult(Result, total=False):
    schedule: dict[str, Any] | None
    schedules: list[dict[str, Any]]
    triggers: list[Trigger]


class LedgerResult(Result, total=False):
    entries: list[dict[str, Any]]
    cursor: int
    has_more: bool


class ObservationResult(Result, total=False):
    epoch: str
    sequence: int
    stream: str
    observations: dict[str, dict[str, Any]]
    events: list[dict[str, Any]]
    subscription_id: str
    expires_at: float
    cursor: int
    gap: bool


class MenuResult(OperationReceipt, total=False):
    menu_id: str
    player: PlayerRef
    step: int
    page: int
    answers: dict[str, Any]
    answer_sources: dict[str, str]
    source: str
    args: dict[str, Any]
    notification: dict[str, Any]
    sessions: list[dict[str, Any]]


class RoleResult(Result, total=False):
    allowed: bool
    roles: list[str]
    assignments: list[dict[str, Any]]
    items: list[dict[str, Any]]
    operation_ids: list[str]


class WorldResult(OperationReceipt, total=False):
    steps: list[dict[str, Any]]
    regions: list[dict[str, Any]]
    losses: list[str]
    receipts: list[dict[str, Any]]
    remote: dict[str, Any]


class ObservationPage(ObservationResult, total=False):
    fragment: str
    next_offset: int | None
    done: bool
    length: int
    sha256: str


class ModerationResult(Result, total=False):
    active: bool
    ban: dict[str, Any] | None
    bans: list[dict[str, Any]]


class MessagingResult(OperationReceipt, total=False):
    routes: list[dict[str, Any]]
    deliveries: list[dict[str, Any]]
    sealed: bool


class ChunkResult(OperationReceipt, total=False):
    data_base64: str
    nbt: dict[str, Any]
    cell: dict[str, Any]
    length: int
    offset: int
    next_offset: int | None
    complete: bool


class ScoreResult(Result, total=False):
    objective: dict[str, Any]
    scores: dict[str, int]
    value: int
    items: list[dict[str, Any]]
    total: int
    found: bool
    result: dict[str, Any]
    operations: list[dict[str, Any]]
    operation_ids: list[str]


class BusinessReceipt(Result, total=False):
    result: dict[str, Any]


class SnapshotPage(TypedDict):
    index: int
    offset: int
    length: int
    sha256: str
    data_base64: str
