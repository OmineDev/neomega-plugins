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
    schema_version: int
    schema: dict[str, Any]
    item: dict[str, Any]


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
    control_id: str
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


class WorldMutationRequest(TypedDict):
    task_id: str
    expected_revision: int


class WorldControlRequest(TypedDict):
    task_id: str
    control_id: str


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


class PlayerRecord(PlayerRef, total=False):
    revision: int
    names: list[dict[str, Any]]
    first_seen: float
    last_seen: float
    source: str
    generation: str


class PlayerResult(Result, total=False):
    player_id: str
    namespace: str
    consistency: str
    schema_version: int | None
    player: PlayerRecord
    players: list[PlayerRef]
    items: list[dict[str, Any]]
    fields_revision: int
    names: list[dict[str, Any]]
    first_seen: float
    last_seen: float
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
    schedule_id: str
    schedule: dict[str, Any] | None
    schedules: list[dict[str, Any]]
    triggers: list[Trigger]


class LedgerResult(Result, total=False):
    entries: list[dict[str, Any]]
    cursor: int
    has_more: bool


class ObservationSample(TypedDict, total=False):
    kind: str
    data: dict[str, Any] | None
    source: str | None
    complete: bool
    observed_at: str | None
    sampled_at: float | None
    sequence: int | None
    epoch: str
    last_success_at: str | None
    last_success_value: dict[str, Any] | None
    last_attempt_at: str
    last_error: dict[str, Any] | None
    attempt_sequence: int
    payload_sequence: int | None
    stale: bool
    state: str
    reason: str


class ObservationResult(Result, total=False):
    removed: bool
    subscriptions: int
    sealed: bool
    inventory_coverage: str
    death_coverage: str
    epoch: str
    sequence: int
    stream: str
    observations: dict[str, ObservationSample]
    events: list[dict[str, Any]]
    subscription_id: str
    expires_at: float
    cursor: int
    gap: bool


class MenuResult(OperationReceipt, total=False):
    ttl_seconds: int
    removed: bool
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
    player_id: str
    scope: str
    total: int
    allowed: bool
    roles: list[str]
    assignments: list[dict[str, Any]]
    items: list[dict[str, Any]]
    operation_ids: list[str]


class RemoteControl(TypedDict, total=False):
    control_id: str
    action: str
    action_state: Literal['accepted', 'unknown', 'completed', 'rejected']
    evidence: str
    observed_state: str | None
    desired_state_observed: bool
    error: str
    error_code: str


class FatalderResult(TypedDict, total=False):
    job_id: str | None
    request_key: str | None
    phase: str | None
    quote: dict[str, Any] | None
    confirm_token: str | None
    confirmation_key: str | None
    accepted_start_key: str | None
    recovery_required: bool
    lease_pending: bool
    task_outcomes: dict[str, Any]
    control: RemoteControl | Literal['submitted']
    controls: dict[str, RemoteControl]


class WorldResult(OperationReceipt, total=False):
    schema_version: int
    fingerprint: str
    kind: str
    epoch: str | None
    created_at: float
    dimension: int | None
    portable: bool
    cancel_requested: bool
    step_count: int
    step_offset: int
    region_count: int
    loss_count: int
    loss_offset: int
    remote_key: str
    remote_call_count: int
    remote_calls: list[dict[str, Any]]
    steps: list[dict[str, Any]]
    regions: list[dict[str, Any]]
    losses: list[str]
    receipts: list[dict[str, Any]]
    remote: FatalderResult | None


class ObservationPage(ObservationResult, total=False):
    offset: int
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
    remote_id: str | None
    updated_at: float
    owner: str
    created_at: float
    direction: str
    limited: bool
    routes: list[dict[str, Any]]
    deliveries: list[dict[str, Any]]
    sealed: bool


class ChunkResult(OperationReceipt, total=False):
    dimension: int
    pos: list[int]
    epoch: str
    created_at: float
    cached_at: float
    action_state: str
    observation: str
    observation_error: str
    observation_not_before: float
    baseline_revision: int
    digest: str
    metadata: dict[str, Any]
    receipt: dict[str, Any]
    data_base64: str
    nbt: dict[str, Any]
    cell: dict[str, Any]
    length: int
    offset: int
    next_offset: int | None
    complete: bool


class ScoreResult(Result, total=False):
    player_id: str
    name: str
    count: int
    sync_state: str
    objective: dict[str, Any]
    scores: dict[str, int]
    value: int | None
    items: list[dict[str, Any]]
    total: int
    found: bool
    result: dict[str, Any]
    operations: list[dict[str, Any]]
    operation_ids: list[str]


class BusinessReceipt(Result, total=False):
    result: dict[str, Any] | None


class SnapshotPage(TypedDict):
    task_id: str
    index: int
    offset: int
    length: int
    sha256: str
    data_base64: str


class PlayerSchemaResult(TypedDict):
    namespace: str
    revision: int
    schema_version: int | None
    schema: dict[str, Any] | None


class PlayerSchemaSaved(PlayerSchemaResult):
    status: str


class BridgeRoute(TypedDict):
    id: str
    prefix: str
    priority: int
    consume: bool
    expires_at: float
    dropped: int


class BridgeSubscription(BridgeRoute):
    cursor: int


class BridgeRemoved(TypedDict):
    removed: bool


class BridgeEvent(TypedDict):
    sequence: int
    text: str
    payload: str
    source_name: str
    connection_id: str
    source_sequence: int
    observed_at: str
    authenticated_player: None
    source: Literal['system_text_untrusted']


class BridgePoll(TypedDict):
    events: list[BridgeEvent]
    cursor: int
    dropped: int
    gap: bool
    expires_at: float


class BridgeStatus(TypedDict):
    stream: str
    sealed: bool
    sequence: int
    routes: list[BridgeRoute]
    total: int
    next_offset: int
    done: bool


class MusicResult(TypedDict):
    playback_id: str
    state: str
    duration: float
    emitted: int
    dropped: int
    error: str | None
    operation_id: str | None
    operation_receipt: dict[str, Any] | None
    operation_intent: dict[str, Any] | None
    position: float
    progress: float
